"""
Main orchestration logic for downloading and processing data
"""
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import config
from client import SharadarClient, YahooFinanceClient
from storage import ParquetStorage
from validators import DataValidator, ValidationError
from utils import setup_logging, get_trading_days


class SharadarDownloader:
    """Main downloader class"""
    
    def __init__(self):
        self.sharadar_client = SharadarClient(config.NASDAQ_API_KEY)
        self.storage = ParquetStorage()
        self.validator = DataValidator()
        
        # Create directories
        Path(config.CACHE_DIR).mkdir(exist_ok=True)
        Path(config.OUTPUT_DIR).mkdir(exist_ok=True)
        
        setup_logging()
        
        # Progress tracking
        self.progress = self._load_progress()
    
    def _load_progress(self) -> dict:
        """Load progress from previous run"""
        if Path(config.PROGRESS_FILE).exists():
            with open(config.PROGRESS_FILE, 'r') as f:
                return json.load(f)
        return {
            'stage': 'not_started',
            'completed_tickers': [],
            'timestamp': None
        }
    
    def _save_progress(self, stage: str, **kwargs):
        """Save progress for interruption recovery"""
        self.progress.update({
            'stage': stage,
            'timestamp': datetime.now().isoformat(),
            **kwargs
        })
        with open(config.PROGRESS_FILE, 'w') as f:
            json.dump(self.progress, f, indent=2)
    
    def _download_tickers(self) -> pd.DataFrame:
        """Step 1: Download and filter ticker universe"""
        if config.VERBOSE:
            print("\n" + "="*60)
            print("STAGE 1: DOWNLOADING TICKER UNIVERSE")
            print("="*60)
        
        # Check if already downloaded
        if Path(config.TICKERS_FILE).exists():
            if config.VERBOSE:
                print("📂 Loading cached tickers table...")
            return pd.read_parquet(config.TICKERS_FILE)
        
        # Download from API
        tickers = self.sharadar_client.download_tickers_table()
        
        # Filter for US exchanges
        tickers = tickers[
            (tickers['exchange'].isin(config.EXCHANGES)) &
            (tickers['table'] == 'SF1')  # Has fundamental data
        ]
        
        # Convert date columns to datetime
        if 'firstpricedate' in tickers.columns:
            tickers['firstpricedate'] = pd.to_datetime(tickers['firstpricedate'])
        if 'lastpricedate' in tickers.columns:
            tickers['lastpricedate'] = pd.to_datetime(tickers['lastpricedate'])
        
        # Save to cache
        self.storage.save_parquet(tickers, config.TICKERS_FILE)
        self._save_progress('tickers_downloaded')
        
        if config.VERBOSE:
            total = len(tickers)
            delisted = (tickers['isdelisted'] == 'Y').sum() if 'isdelisted' in tickers.columns else 0
            print(f"✅ Filtered to {total:,} US tickers ({delisted:,} delisted)")
        
        return tickers
    
    def _download_sf1(self) -> pd.DataFrame:
        """Step 2: Download SF1 fundamentals"""
        if config.VERBOSE:
            print("\n" + "="*60)
            print("STAGE 2: DOWNLOADING SF1 FUNDAMENTALS")
            print("="*60)
        
        # Check if already downloaded
        if Path(config.SF1_RAW_FILE).exists():
            if config.VERBOSE:
                print("📂 Loading cached SF1 data...")
            return pd.read_parquet(config.SF1_RAW_FILE)
        
        # Download from API (already in wide format)
        sf1 = self.sharadar_client.download_sf1_bulk()
        
        # Data is already in WIDE format from bulk export
        # No need to pivot - indicators are already columns
        
        # Filter date range
        sf1_filtered = sf1[
            (sf1['date'] >= config.START_DATE) &
            (sf1['date'] <= config.END_DATE)
        ].copy()
        
        # Save to cache
        self.storage.save_parquet(sf1_filtered, config.SF1_RAW_FILE)
        self._save_progress('sf1_downloaded')
        
        if config.VERBOSE:
            print(f"✅ Filtered to {len(sf1_filtered):,} records in date range")
        
        return sf1_filtered
    
    def _download_yahoo_prices(self, tickers: pd.DataFrame) -> pd.DataFrame:
        """Step 3: Download Yahoo Finance prices"""
        if config.VERBOSE:
            print("\n" + "="*60)
            print("STAGE 3: DOWNLOADING YAHOO FINANCE PRICES")
            print("="*60)
        
        # Check if already downloaded
        if Path(config.YAHOO_PRICES_FILE).exists():
            if config.VERBOSE:
                print("📂 Loading cached Yahoo Finance data...")
            return pd.read_parquet(config.YAHOO_PRICES_FILE)
        
        # Get unique ticker list
        ticker_list = tickers['ticker'].unique().tolist()
        
        # Download prices with metadata for error categorization
        prices = YahooFinanceClient.download_multiple_tickers(
            ticker_list,
            config.START_DATE,
            config.END_DATE,
            ticker_metadata=tickers  # Pass metadata for error categorization
        )
        
        # Save to cache
        self.storage.save_parquet(prices, config.YAHOO_PRICES_FILE)
        self._save_progress('yahoo_downloaded')
        
        return prices
    
    def _process_master_file(
        self,
        sf1: pd.DataFrame,
        prices: pd.DataFrame,
        tickers: pd.DataFrame
    ) -> pd.DataFrame:
        """Step 4: Process and merge data - KEEPS ALL DELISTED STOCKS"""
        if config.VERBOSE:
            print("\n" + "="*60)
            print("STAGE 4: PROCESSING MASTER FILE")
            print("="*60)
        
        # Forward-fill sharesbas to daily frequency
        if config.VERBOSE:
            print("🔄 Forward-filling sharesbas to daily frequency...")
        
        sf1_daily = sf1.sort_values(['ticker', 'date'])
        # Column name is lowercase 'sharesbas' from bulk export
        sf1_daily['sharesbas'] = sf1_daily.groupby('ticker')['sharesbas'].ffill()
        
        # Convert dates for joining
        sf1_daily['date'] = pd.to_datetime(sf1_daily['date']).dt.date
        
        # Only merge with prices if we have them
        if not prices.empty and len(prices) > 0:
            prices['date'] = pd.to_datetime(prices['date']).dt.date
            
            # Join with prices (LEFT JOIN - keeps ALL SF1 rows, including delisted stocks)
            if config.VERBOSE:
                print("🔗 Joining fundamentals with prices...")
            
            master = sf1_daily.merge(
                prices[['ticker', 'date', 'open_price']],
                on=['ticker', 'date'],
                how='left'  # Critical: LEFT join keeps all SF1 data
            )
            
            # Calculate market cap (will be NULL for delisted stocks without price data - that's OK!)
            master['market_cap'] = master['sharesbas'] * master['open_price']
        else:
            # No prices - just use SF1 data
            if config.VERBOSE:
                print("ℹ️  No price data - outputting fundamentals only")
            
            master = sf1_daily.copy()
            master['open_price'] = None  # Will be added later
            master['market_cap'] = None  # Will be calculated later
        
        # Add delisting metadata (for reference only - NOT for filtering)
        if config.VERBOSE:
            print("📅 Adding delisting metadata (not filtering)...")
        
        # Add isdelisted flag from tickers table
        ticker_metadata = tickers[['ticker', 'isdelisted']].drop_duplicates()
        master = master.merge(
            ticker_metadata,
            on='ticker',
            how='left'
        )
        
        # Add lastpricedate from tickers (for reference)
        if 'lastpricedate' in tickers.columns:
            ticker_lastprice = tickers[['ticker', 'lastpricedate']].drop_duplicates()
            master = master.merge(
                ticker_lastprice,
                on='ticker',
                how='left',
                suffixes=('', '_sharadar')
            )
        
        # Get last trade date from Yahoo for each ticker (for reference) - only if we have prices
        if not prices.empty and len(prices) > 0:
            last_yahoo_dates = prices.groupby('ticker')['date'].max().reset_index()
            last_yahoo_dates.columns = ['ticker', 'last_yahoo_date']
            master = master.merge(
                last_yahoo_dates,
                on='ticker',
                how='left'
            )
            # Add flag for whether this row has price data
            master['has_price_data'] = master['open_price'].notna()
        else:
            master['last_yahoo_date'] = None
            master['has_price_data'] = False
        
        # Sort by date and ticker
        master = master.sort_values(['date', 'ticker']).reset_index(drop=True)
        
        if config.VERBOSE:
            print(f"✅ Master file created: {len(master):,} rows")
            
            # Statistics
            total_tickers = master['ticker'].nunique()
            
            if 'isdelisted' in master.columns:
                delisted_tickers = master[master['isdelisted'] == 'Y']['ticker'].nunique()
                active_tickers = master[master['isdelisted'] == 'N']['ticker'].nunique()
                print(f"   Total unique tickers: {total_tickers:,}")
                print(f"   Active tickers: {active_tickers:,} ({active_tickers/total_tickers*100:.1f}%)")
                print(f"   Delisted tickers: {delisted_tickers:,} ({delisted_tickers/total_tickers*100:.1f}%)")
            
            # Price data coverage
            if config.SKIP_YAHOO_DOWNLOAD:
                print(f"   Price data: NOT DOWNLOADED (will be added later)")
                print(f"   Shares outstanding: FORWARD-FILLED for daily frequency")
            else:
                rows_with_price = master['has_price_data'].sum()
                rows_without_price = (~master['has_price_data']).sum()
                price_coverage_pct = rows_with_price / len(master) * 100
                
                print(f"   Rows with price data: {rows_with_price:,} ({price_coverage_pct:.1f}%)")
                print(f"   Rows without price data: {rows_without_price:,} ({100-price_coverage_pct:.1f}%)")
                
                # Market cap statistics
                null_market_cap = master['market_cap'].isna().sum()
                null_pct = null_market_cap / len(master) * 100
                print(f"   Market cap nulls: {null_market_cap:,} ({null_pct:.1f}%)")
            
            # Confirm no filtering happened
            print(f"\n   ⚠️  IMPORTANT: NO ROWS WERE FILTERED - all fundamental data preserved")
        
        return master
    
    def _validate_and_save_master(self, master: pd.DataFrame):
        """Step 5: Validate and save master file"""
        if config.VERBOSE:
            print("\n" + "="*60)
            print("STAGE 5: VALIDATION")
            print("="*60)
        
        # Run validation
        validation_result = self.validator.validate_dataset(master)
        
        # Write validation log
        with open(config.VALIDATION_LOG, 'w') as f:
            f.write("="*60 + "\n")
            f.write("SHARADAR DOWNLOADER - VALIDATION REPORT\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
            f.write("="*60 + "\n\n")
            
            if validation_result['warnings']:
                f.write("WARNINGS:\n")
                f.write("-"*60 + "\n")
                for warning in validation_result['warnings']:
                    f.write(f"{warning}\n")
                    if config.VERBOSE:
                        print(f"⚠️  {warning}")
                f.write("\n")
            
            if validation_result['errors']:
                f.write("ERRORS:\n")
                f.write("-"*60 + "\n")
                for error in validation_result['errors']:
                    f.write(f"{error}\n")
                    if config.VERBOSE:
                        print(f"🛑 {error}")
                f.write("\n")
            
            f.write(f"\nValidation Status: {'PASSED' if validation_result['passed'] else 'FAILED'}\n")
        
        # Fail if critical errors
        if not validation_result['passed']:
            raise ValidationError(
                f"Validation failed with {len(validation_result['errors'])} critical errors. "
                f"See {config.VALIDATION_LOG} for details."
            )
        
        if config.VERBOSE:
            print(f"\n✅ VALIDATION PASSED")
            print(f"   Log saved to: {config.VALIDATION_LOG}")
        
        # Save master file
        if config.VERBOSE:
            print(f"\n💾 Saving master file to {config.MASTER_FILE}...")
        
        self.storage.save_parquet(master, config.MASTER_FILE)
        self._save_progress('master_created')
    
    def _split_to_daily_files(self, master: pd.DataFrame):
        """Step 6: Split master file into daily Parquets"""
        if config.VERBOSE:
            print("\n" + "="*60)
            print("STAGE 6: SPLITTING TO DAILY FILES")
            print("="*60)
        
        unique_dates = sorted(master['date'].unique())
        
        if config.VERBOSE:
            print(f"📅 Splitting {len(unique_dates):,} trading days...")
        
        from tqdm import tqdm
        for date in tqdm(unique_dates, disable=not config.VERBOSE):
            daily_df = master[master['date'] == date].copy()
            
            # Format filename as YYYY-MM-DD.parquet
            filename = f"{date}.parquet"
            filepath = Path(config.OUTPUT_DIR) / filename
            
            self.storage.save_parquet(daily_df, str(filepath))
        
        self._save_progress('split_completed')
        
        if config.VERBOSE:
            print(f"✅ Daily files saved to {config.OUTPUT_DIR}/")
    
    def _export_ticker_universe_csv(self, tickers: pd.DataFrame):
        """Export complete ticker universe to CSV (ticker symbols only)"""
        if config.VERBOSE:
            print("\n" + "="*60)
            print("EXPORTING TICKER UNIVERSE CSV")
            print("="*60)
        
        # Create DataFrame with only ticker column
        export_df = pd.DataFrame({'symbol': tickers['ticker']})
        
        # Sort alphabetically
        export_df = export_df.sort_values('symbol')
        
        # Remove duplicates (if any)
        export_df = export_df.drop_duplicates()
        
        # Define output path
        csv_path = Path(config.OUTPUT_DIR) / "ticker_universe.csv"
        
        # Export to CSV
        export_df.to_csv(csv_path, index=False)
        
        if config.VERBOSE:
            total_tickers = len(export_df)
            delisted_count = (tickers['isdelisted'] == 'Y').sum() if 'isdelisted' in tickers.columns else 0
            active_count = len(tickers) - delisted_count
            
            print(f"✅ Ticker universe exported to: {csv_path}")
            print(f"   Total tickers: {total_tickers:,}")
            print(f"   Active: {active_count:,}")
            print(f"   Delisted: {delisted_count:,}")
    
    def _export_ticker_universe_symbols_csv(self, tickers: pd.DataFrame):
        """Export all ticker symbols (single column, includes delisted)"""
        if config.VERBOSE:
            print("\n" + "="*60)
            print("EXPORTING TICKER SYMBOLS CSV")
            print("="*60)
        
        # Create DataFrame with only ticker column (ALL tickers)
        export_df = pd.DataFrame({'symbol': tickers['ticker']})
        
        # Sort alphabetically
        export_df = export_df.sort_values('symbol')
        
        # Remove duplicates (if any)
        export_df = export_df.drop_duplicates()
        
        # Define output path
        csv_path = Path(config.OUTPUT_DIR) / "ticker_universe_symbols.csv"
        
        # Export to CSV
        export_df.to_csv(csv_path, index=False)
        
        if config.VERBOSE:
            total_tickers = len(export_df)
            delisted_count = (tickers['isdelisted'] == 'Y').sum() if 'isdelisted' in tickers.columns else 0
            active_count = len(tickers) - delisted_count
            
            print(f"✅ Ticker symbols exported to: {csv_path}")
            print(f"   Total tickers: {total_tickers:,}")
            print(f"   Active: {active_count:,}")
            print(f"   Delisted: {delisted_count:,}")

    def _export_ticker_universe_verbose_csv(self, tickers: pd.DataFrame):
        """Export complete ticker universe with all metadata (includes delisted)"""
        if config.VERBOSE:
            print("\n" + "="*60)
            print("EXPORTING VERBOSE TICKER UNIVERSE CSV")
            print("="*60)
        
        # Select relevant columns for verbose export
        export_columns = [
            'ticker',
            'name',
            'exchange',
            'isdelisted',
            'category',
            'sector',
            'industry',
            'scalemarketcap',
            'siccode',
            'sicindustry',
            'famasector',
            'famaindustry',
            'currency',
            'location',
            'firstpricedate',
            'lastpricedate',
            'firstquarter',
            'lastquarter',
            'secfilings',
            'companysite',
            'delisted',
            'permaticker',
            'relatedtickers'
        ]
        
        # Filter to only existing columns
        available_columns = [col for col in export_columns if col in tickers.columns]
        export_df = tickers[available_columns].copy()
        
        # Sort by delisting status (active first), then exchange, then ticker
        export_df = export_df.sort_values(['isdelisted', 'exchange', 'ticker'])
        
        # Define output path
        csv_path = Path(config.OUTPUT_DIR) / "ticker_universe_verbose.csv"
        
        # Export to CSV
        export_df.to_csv(csv_path, index=False)
        
        if config.VERBOSE:
            total_tickers = len(export_df)
            delisted_count = (export_df['isdelisted'] == 'Y').sum() if 'isdelisted' in export_df.columns else 0
            active_count = total_tickers - delisted_count
            
            print(f"✅ Verbose ticker universe exported to: {csv_path}")
            print(f"   Total tickers: {total_tickers:,}")
            print(f"   Active: {active_count:,}")
            print(f"   Delisted: {delisted_count:,}")
            
            # Show exchange breakdown
            if 'exchange' in export_df.columns:
                print(f"\n   Exchange breakdown:")
                for exchange, count in export_df['exchange'].value_counts().items():
                    print(f"      {exchange}: {count:,}")
    
    def run(self):
        """Main execution method"""
        try:
            start_time = datetime.now()
            
            if config.VERBOSE:
                print("\n" + "🚀 " + "="*56 + " 🚀")
                print("   SHARADAR DOWNLOADER - PRODUCTION RUN")
                print("🚀 " + "="*56 + " 🚀")
                print(f"\nConfiguration:")
                print(f"  Date Range: {config.START_DATE} to {config.END_DATE}")
                print(f"  Dimension: {config.DIMENSION}")
                print(f"  Indicators: {', '.join(config.INDICATORS)}")
                if config.SKIP_YAHOO_DOWNLOAD:
                    print(f"  Price Source: SKIPPED - will be added from another API later")
                else:
                    print(f"  Price Field: {config.PRICE_FIELD}")
                print(f"  Exchanges: {', '.join(config.EXCHANGES)}")
            
            # Execute pipeline
            tickers = self._download_tickers()
            self._export_ticker_universe_symbols_csv(tickers)
            self._export_ticker_universe_verbose_csv(tickers)
            sf1 = self._download_sf1()
            
            # Conditionally download Yahoo prices
            if config.SKIP_YAHOO_DOWNLOAD:
                if config.VERBOSE:
                    print("\n" + "="*60)
                    print("STAGE 3: SKIPPING YAHOO FINANCE (per config)")
                    print("="*60)
                    print("   ℹ️  Price data will be added from another API later")
                    print("   ℹ️  Outputting ticker universe with daily shares outstanding")
                prices = pd.DataFrame(columns=['ticker', 'date', 'open_price'])  # Empty DataFrame
            else:
                prices = self._download_yahoo_prices(tickers)
            
            master = self._process_master_file(sf1, prices, tickers)
            self._validate_and_save_master(master)
            self._split_to_daily_files(master)
            
            # Summary
            elapsed = datetime.now() - start_time
            
            if config.VERBOSE:
                print("\n" + "="*60)
                print("✅ DOWNLOAD COMPLETE")
                print("="*60)
                print(f"Total Runtime: {elapsed}")
                print(f"Tickers Processed: {master['ticker'].nunique():,}")
                print(f"Date Range: {master['date'].min()} to {master['date'].max()}")
                print(f"Total Records: {len(master):,}")
                print(f"Daily Files Created: {len(list(Path(config.OUTPUT_DIR).glob('*.parquet'))):,}")
                print(f"Master File: {config.MASTER_FILE}")
                print(f"Validation Log: {config.VALIDATION_LOG}")
                if config.SKIP_YAHOO_DOWNLOAD:
                    print(f"\n💡 Next Step: Add price data from your chosen API")
                    print(f"   Columns to add: open_price, market_cap")
                print("="*60 + "\n")
        
        except Exception as e:
            if config.VERBOSE:
                print(f"\n❌ ERROR: {e}")
            raise
