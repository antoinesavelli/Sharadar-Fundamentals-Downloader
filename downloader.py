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
        
        # Convert date columns
        tickers['firstpricedate'] = pd.to_datetime(tickers['firstpricedate'])
        tickers['delisted'] = pd.to_datetime(tickers['delisted'], errors='coerce')
        
        # Save to cache
        self.storage.save_parquet(tickers, config.TICKERS_FILE)
        self._save_progress('tickers_downloaded')
        
        if config.VERBOSE:
            total = len(tickers)
            delisted = tickers['isdelisted'].sum()
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
        
        # Download from API
        sf1 = self.sharadar_client.download_sf1_bulk()
        
        # Pivot to wide format (one row per ticker-date)
        sf1_wide = sf1.pivot_table(
            index=['ticker', 'date'],
            columns='indicator',
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Filter date range
        sf1_wide = sf1_wide[
            (sf1_wide['date'] >= config.START_DATE) &
            (sf1_wide['date'] <= config.END_DATE)
        ]
        
        # Save to cache
        self.storage.save_parquet(sf1_wide, config.SF1_RAW_FILE)
        self._save_progress('sf1_downloaded')
        
        if config.VERBOSE:
            print(f"✅ Filtered to {len(sf1_wide):,} records in date range")
        
        return sf1_wide
    
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
        
        # Download prices
        prices = YahooFinanceClient.download_multiple_tickers(
            ticker_list,
            config.START_DATE,
            config.END_DATE
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
        """Step 4: Process and merge data"""
        if config.VERBOSE:
            print("\n" + "="*60)
            print("STAGE 4: PROCESSING MASTER FILE")
            print("="*60)
        
        # Forward-fill SHARESBAS to daily frequency
        if config.VERBOSE:
            print("🔄 Forward-filling SHARESBAS to daily frequency...")
        
        sf1_daily = sf1.sort_values(['ticker', 'date'])
        sf1_daily['SHARESBAS'] = sf1_daily.groupby('ticker')['SHARESBAS'].ffill()
        
        # Convert dates for joining
        sf1_daily['date'] = pd.to_datetime(sf1_daily['date']).dt.date
        prices['date'] = pd.to_datetime(prices['date']).dt.date
        
        # Join with prices (LEFT JOIN - keep all SF1 rows)
        if config.VERBOSE:
            print("🔗 Joining fundamentals with prices...")
        
        master = sf1_daily.merge(
            prices[['ticker', 'date', 'open_price']],
            on=['ticker', 'date'],
            how='left'
        )
        
        # Calculate market cap (will be NULL if price is NULL)
        master['market_cap'] = master['SHARESBAS'] * master['open_price']
        
        # Add delisting information
        if config.VERBOSE:
            print("📅 Processing delisting dates...")
        
        # Get last trade date from Yahoo for each ticker
        last_trade_dates = prices.groupby('ticker')['date'].max().to_dict()
        
        # Get Sharadar delisting dates
        sharadar_delistings = tickers.set_index('ticker')['delisted'].to_dict()
        
        # Use earlier of Yahoo last trade or Sharadar delisting
        def get_effective_delisting(ticker):
            yahoo_last = last_trade_dates.get(ticker)
            sharadar_last = sharadar_delistings.get(ticker)
            
            if pd.isna(sharadar_last):
                return yahoo_last  # Still listed or Yahoo is authority
            if yahoo_last is None:
                return pd.to_datetime(sharadar_last).date()
            
            return min(yahoo_last, pd.to_datetime(sharadar_last).date())
        
        master['effective_delisting'] = master['ticker'].apply(get_effective_delisting)
        
        # Filter out data after effective delisting
        master = master[
            (master['effective_delisting'].isna()) |
            (master['date'] <= master['effective_delisting'])
        ]
        
        # Clean up
        master = master.drop(columns=['effective_delisting'])
        master = master.sort_values(['date', 'ticker']).reset_index(drop=True)
        
        if config.VERBOSE:
            print(f"✅ Master file created: {len(master):,} rows")
            null_count = master['market_cap'].isna().sum()
            null_pct = null_count / len(master) * 100
            print(f"   Market cap nulls: {null_count:,} ({null_pct:.2f}%)")
        
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
                print(f"  Price Field: {config.PRICE_FIELD}")
                print(f"  Exchanges: {', '.join(config.EXCHANGES)}")
            
            # Execute pipeline
            tickers = self._download_tickers()
            sf1 = self._download_sf1()
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
                print("="*60 + "\n")
            
        except Exception as e:
            if config.VERBOSE:
                print(f"\n❌ ERROR: {e}")
            raise
