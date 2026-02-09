"""
API clients for Sharadar and Yahoo Finance
"""
import nasdaqdatalink
import yfinance as yf
import pandas as pd
import time
import requests
import zipfile
import io
from typing import List, Optional
from tqdm import tqdm
import config
import logging


class SharadarClient:
    """Client for Sharadar API via Nasdaq Data Link"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        
        # Configure the official package with SAFER settings
        nasdaqdatalink.ApiConfig.api_key = api_key
        nasdaqdatalink.ApiConfig.retry_backoff_factor = 2
        nasdaqdatalink.ApiConfig.retry_status_codes = [500, 502, 503, 504]  # REMOVED 429 - handle manually
        nasdaqdatalink.ApiConfig.number_of_retries = 3
        
        self.last_request_time = 0
        self.min_request_interval = config.API_REQUEST_DELAY
        self.last_bulk_export_time = 0
    
    def _wait_for_rate_limit(self):
        """
        Proactive rate limiting - wait before making request.
        
        Nasdaq limits: 5,000 calls per 10 minutes = 8.33 calls/second max.
        We use 2 second delay = 0.5 calls/second (well under limit).
        """
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            wait_time = self.min_request_interval - elapsed
            if config.VERBOSE and wait_time > 0.5:
                print(f"⏳ Rate limit cooldown: {wait_time:.1f}s...")
            time.sleep(wait_time)
        self.last_request_time = time.time()
    
    def _wait_for_bulk_export_limit(self):
        """
        Wait for bulk export cooldown.
        
        Nasdaq limit: 10 bulk exports per hour = 1 every 6 minutes.
        """
        elapsed = time.time() - self.last_bulk_export_time
        if elapsed < config.BULK_EXPORT_COOLDOWN:
            wait_time = config.BULK_EXPORT_COOLDOWN - elapsed
            if config.VERBOSE:
                print(f"⏰ Bulk export cooldown: {wait_time:.1f}s ({wait_time/60:.1f} min)...")
            time.sleep(wait_time)
        self.last_bulk_export_time = time.time()
    
    def _download_with_pagination(self, table_name: str, **filters) -> pd.DataFrame:
        """
        Download table using manual pagination (avoids bulk export rate limits).
        
        Pagination uses standard API quota: 5,000 calls per 10 minutes.
        Each page = 1 API call. TICKERS table = ~8 pages total.
        """
        if config.VERBOSE:
            print(f"📄 Using pagination for {table_name}...")
        
        url = f"https://data.nasdaq.com/api/v3/datatables/{table_name}.json"
        all_data = []
        cursor = None
        page = 1
        columns = None
        
        while True:
            self._wait_for_rate_limit()
            
            params = {"api_key": self.api_key}
            params.update(filters)
            
            if cursor:
                params['qopts.cursor_id'] = cursor
            
            try:
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 429:
                    data = response.json()
                    error_code = data.get('quandl_error', {}).get('code', 'UNKNOWN')
                    
                    if 'QELx04' in str(error_code):
                        # 10-minute limit
                        print("Hit 10-minute call limit (5,000 calls)")
                        raise Exception(
                            f"Rate limited. Wait 10+ minutes before retrying. "
                            f"If problem persists, contact clientsuccess@nasdaq.com"
                        )
                    elif 'QELx03' in str(error_code):
                        # Daily limit
                        print("Hit daily call limit (720,000 calls)")
                        raise Exception(
                            f"Daily call limit reached. Try again later."
                        )
                
                response.raise_for_status()
                data = response.json()
                
                # Extract rows and columns
                rows = data['datatable']['data']
                if not rows:
                    break
                
                if not columns:  # First page - get column names
                    columns = [col['name'] for col in data['datatable']['columns']]
                
                all_data.extend(rows)
                
                # Check for next page
                cursor = data['meta'].get('next_cursor_id')
                
                if config.VERBOSE:
                    print(f"   Page {page}: {len(all_data):,} rows downloaded")
                
                if not cursor:
                    break
                
                page += 1
                
            except requests.exceptions.RequestException as e:
                if config.VERBOSE:
                    print(f"❌ Error during pagination: {e}")
                raise
        
        if not all_data:
            raise ValueError(f"No data downloaded from {table_name}")
        
        return pd.DataFrame(all_data, columns=columns)
    
    def _download_bulk_export(self, table_name: str, **filters) -> pd.DataFrame:
        """
        Download large table using async bulk export.
        
        Bulk export limit: 10 per hour (separate from standard API quota).
        Each export request = 1 of your 10/hour allocation.
        """
        if config.VERBOSE:
            print(f"📦 Using bulk export for {table_name}...")
        
        # Wait for bulk export cooldown (10 per hour = 1 every 6 minutes)
        self._wait_for_bulk_export_limit()
        
        url = f"https://data.nasdaq.com/api/v3/datatables/{table_name}.json"
        params = {"api_key": self.api_key, "qopts.export": "true"}
        params.update(filters)
        
        # Retry logic with exponential backoff
        for attempt in range(config.MAX_RETRIES):
            try:
                # Step 1: Request export job
                if config.VERBOSE and attempt > 0:
                    print(f"   Retry attempt {attempt + 1}/{config.MAX_RETRIES}")
                
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 429:
                    # Rate limited on bulk export - hit 10 exports in last hour
                    if config.VERBOSE:
                        print(f"\n❌ BULK EXPORT RATE LIMITED")
                        print(f"   You've used 10 bulk exports in the last hour.")
                        print(f"   Current time: {time.strftime('%H:%M:%S')}")
                        print(f"   Retry after: {time.strftime('%H:%M:%S', time.localtime(time.time() + 3600))}")
                    
                    # Don't retry - wait the full hour
                    raise Exception(
                        f"Bulk export rate limited. You can only do 10 bulk exports per hour. "
                        f"Wait 60+ minutes before retrying."
                    )
                
                response.raise_for_status()
                data = response.json()
                
                # Step 2: Extract download link
                file_info = data['datatable_bulk_download']['file']
                download_url = file_info['link']
                status = file_info['status']
                
                if config.VERBOSE:
                    print(f"   Export job created. Status: {status}")
                
                # Step 3: Poll until export is ready
                elapsed = 0
                # Poll for any non-final status (creating, regenerating, etc.)
                while status not in ["fresh", "failed"] and elapsed < config.EXPORT_MAX_WAIT:
                    if config.VERBOSE:
                        print(f"   Waiting for export... Status: {status} ({elapsed}s)")
                    time.sleep(config.EXPORT_POLL_INTERVAL)
                    elapsed += config.EXPORT_POLL_INTERVAL
                    
                    # Check status (doesn't count against rate limit)
                    try:
                        check_response = requests.get(download_url, timeout=30)
                        if check_response.status_code == 200:
                            check_data = check_response.json()
                            # Handle both direct status or nested file status
                            if 'datatable_bulk_download' in check_data:
                                status = check_data['datatable_bulk_download']['file']['status']
                            elif 'file' in check_data:
                                status = check_data['file']['status']
                            else:
                                status = check_data.get('status', status)
                    except Exception as poll_error:
                        if config.VERBOSE:
                            print(f"   Poll error (will retry): {poll_error}")
                
                if status == "failed":
                    raise Exception(f"Export failed on server side")
                elif status != "fresh":
                    raise Exception(f"Export timeout. Last status: {status}. Try increasing EXPORT_MAX_WAIT in config.")
                
                # Step 4: Download zip file
                if config.VERBOSE:
                    print(f"   Export ready. Downloading ZIP...")
                
                zip_response = requests.get(download_url)
                zip_response.raise_for_status()
                
                # Step 5: Extract CSV from ZIP
                with zipfile.ZipFile(io.BytesIO(zip_response.content)) as z:
                    csv_filename = z.namelist()[0]
                    with z.open(csv_filename) as f:
                        df = pd.read_csv(f)
                
                if config.VERBOSE:
                    print(f"✅ Bulk export complete: {len(df):,} rows")
                
                return df
                
            except requests.exceptions.RequestException as e:
                if config.VERBOSE:
                    print(f"❌ Bulk export error: {e}")
                
                if attempt == config.MAX_RETRIES - 1:
                    raise
        
        raise Exception(f"Failed to download {table_name} after {config.MAX_RETRIES} attempts")
    
    def download_tickers_table(self) -> pd.DataFrame:
        """
        Download SHARADAR/TICKERS table using pagination.
        
        TICKERS is small (~8K rows = ~8 API calls with pagination).
        Uses standard API quota, not bulk export quota.
        """
        if config.VERBOSE:
            print("📋 Downloading SHARADAR/TICKERS table...")
        
        try:
            # Use pagination instead of bulk export
            df = self._download_with_pagination('SHARADAR/TICKERS')
            
            if config.VERBOSE:
                print(f"✅ Downloaded {len(df):,} tickers")
            
            return df
            
        except Exception as e:
            if config.VERBOSE:
                print(f"❌ Error downloading tickers: {e}")
            raise
    
    def download_sf1_bulk(self) -> pd.DataFrame:
        """
        Download SF1 fundamentals via bulk export.
        
        SF1 is large (~20M+ rows) so bulk export is necessary.
        Uses 1 of your 10 bulk exports per hour.
        
        Note: Bulk exports return data in WIDE format (indicators as columns),
        not LONG format (indicator column with values).
        """
        if config.VERBOSE:
            print("📊 Downloading SHARADAR/SF1 bulk export...")
            print(f"   Dimension: {config.DIMENSION}")
            if config.INDICATORS:
                print(f"   Will select indicators: {', '.join(config.INDICATORS)}")
        
        try:
            # Build filter parameters
            # NOTE: Bulk exports only support dimension filter, not indicator filter
            filters = {
                'dimension': config.DIMENSION,
            }
            
            # Use bulk export with proper rate limiting
            df = self._download_bulk_export('SHARADAR/SF1', **filters)
            
            if config.VERBOSE:
                print(f"   Downloaded {len(df):,} rows with {len(df.columns)} columns")
            
            # Filter by indicators AFTER download
            # Bulk export returns WIDE format: each indicator is a column
            if config.INDICATORS:
                # Keep core columns plus requested indicators
                core_columns = ['ticker', 'dimension', 'calendardate', 'datekey', 'reportperiod', 'lastupdated']
                available_core = [col for col in core_columns if col in df.columns]
                
                # Case-insensitive matching for indicators
                # Convert both config indicators and df columns to lowercase for comparison
                indicators_lower = [ind.lower() for ind in config.INDICATORS]
                available_indicators = [col for col in df.columns if col.lower() in indicators_lower]
                
                if not available_indicators:
                    raise ValueError(
                        f"None of the requested indicators {config.INDICATORS} found in dataset. "
                        f"Available columns: {list(df.columns)}"
                    )
                
                # Select only needed columns
                columns_to_keep = available_core + available_indicators
                df = df[columns_to_keep]
                
                if config.VERBOSE:
                    requested_set = set(ind.lower() for ind in config.INDICATORS)
                    found_set = set(col.lower() for col in available_indicators)
                    missing = requested_set - found_set
                    if missing:
                        print(f"   ⚠️  Missing indicators: {missing}")
                    print(f"   Filtered to {len(columns_to_keep)} columns ({len(available_indicators)} indicators)")
            
            # Convert date column
            if 'datekey' in df.columns:
                df['date'] = pd.to_datetime(df['datekey'])
            elif 'calendardate' in df.columns:
                df['date'] = pd.to_datetime(df['calendardate'])
            
            if config.VERBOSE:
                print(f"✅ Downloaded {len(df):,} fundamental records")
            
            return df
            
        except Exception as e:
            if config.VERBOSE:
                print(f"❌ Error downloading SF1 data: {e}")
            raise
    
    def download_indicators_metadata(self) -> pd.DataFrame:
        """Download SF1 indicators metadata using pagination"""
        if config.VERBOSE:
            print("📖 Downloading SF1 indicators metadata...")
        
        try:
            df = self._download_with_pagination('SHARADAR/INDICATORS')
            
            if config.VERBOSE:
                print(f"✅ Downloaded {len(df):,} indicator definitions")
            
            return df
            
        except Exception as e:
            if config.VERBOSE:
                print(f"❌ Error downloading indicators metadata: {e}")
            raise


class YahooFinanceClient:
    """Client for Yahoo Finance data"""
    
    # Suppress yfinance's verbose logging
    logging.getLogger('yfinance').setLevel(logging.CRITICAL)
    
    @staticmethod
    def download_ticker_history(
        ticker: str,
        start_date: str,
        end_date: str,
        ticker_metadata: Optional[pd.DataFrame] = None
    ) -> Optional[pd.DataFrame]:
        """Download historical OHLCV data for a single ticker
        
        Handles special suffixes (.U, .WS, etc.) that Yahoo may not recognize
        Categorizes failures for better error reporting
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date for download
            end_date: End date for download
            ticker_metadata: Optional DataFrame with ticker info (for error categorization)
        """
        try:
            # Try original ticker first (suppress yfinance output)
            ticker_obj = yf.Ticker(ticker)
            hist = ticker_obj.history(start=start_date, end=end_date, progress=False)
            
            # If empty and ticker has a suffix, try without it
            if hist.empty and '.' in ticker:
                base_ticker = ticker.split('.')[0]
                ticker_obj = yf.Ticker(base_ticker)
                hist = ticker_obj.history(start=start_date, end=end_date, progress=False)
                
                if not hist.empty:
                    ticker = base_ticker
            
            if hist.empty:
                YahooFinanceClient._log_download_failure(ticker, "No data available", ticker_metadata, start_date)
                return None
            
            # Extract only what we need
            df = pd.DataFrame({
                'date': hist.index,
                'ticker': ticker,
                'open_price': hist['Open'].values
            })
            
            df['date'] = pd.to_datetime(df['date']).dt.date
            return df
            
        except Exception as e:
            # If ticker has suffix, try base ticker before giving up
            if '.' in ticker:
                try:
                    base_ticker = ticker.split('.')[0]
                    ticker_obj = yf.Ticker(base_ticker)
                    hist = ticker_obj.history(start=start_date, end=end_date, progress=False)
                    
                    if not hist.empty:
                        df = pd.DataFrame({
                            'date': hist.index,
                            'ticker': base_ticker,
                            'open_price': hist['Open'].values
                        })
                        df['date'] = pd.to_datetime(df['date']).dt.date
                        return df
                except:
                    pass
            
            YahooFinanceClient._log_download_failure(ticker, str(e), ticker_metadata, start_date)
            return None
    
    @staticmethod
    def _log_download_failure(ticker: str, error: str, metadata: Optional[pd.DataFrame], start_date: str):
        """Categorize and log download failures with context"""
        if not config.VERBOSE:
            return
        
        # Check for SPAC units and special securities FIRST
        if '.' in ticker:
            suffix = ticker.split('.')[-1]
            if suffix in ['U', 'UN']:
                print(f"⚠️  {ticker}: SPAC Unit (suffix .{suffix})")
                return
            elif suffix in ['WS', 'WT', 'W']:
                print(f"⚠️  {ticker}: Warrant (suffix .{suffix})")
                return
            elif suffix in ['RT', 'R']:
                print(f"⚠️  {ticker}: Rights (suffix .{suffix})")
                return
        
        # Pattern: Ends with 'U' (SPAC units without period)
        if ticker.endswith('U') and len(ticker) > 4:
            print(f"⚠️  {ticker}: Likely SPAC Unit (ends with U)")
            return
        
        # Pattern: Known SPAC/warrant patterns
        if ticker.endswith(('WT', 'WS', 'RT')):
            print(f"⚠️  {ticker}: Likely Warrant/Rights")
            return
        
        # Check metadata if available
        if metadata is not None and isinstance(metadata, pd.DataFrame):
            ticker_matches = metadata[metadata['ticker'] == ticker]
            if len(ticker_matches) > 0:
                ticker_info = ticker_matches.iloc[0]
                
                # Check Sharadar's isdelisted field
                if 'isdelisted' in ticker_info and ticker_info['isdelisted'] == 'Y':
                    # Sharadar says it's delisted
                    if 'lastpricedate' in ticker_info and pd.notna(ticker_info['lastpricedate']):
                        try:
                            last_price = pd.to_datetime(ticker_info['lastpricedate'])
                            start = pd.to_datetime(start_date)
                            
                            if last_price < start:
                                years_before = (start - last_price).days / 365.25
                                print(f"⚠️  {ticker}: Delisted {years_before:.1f}y before {start_date[:4]}")
                                return
                            else:
                                print(f"⚠️  {ticker}: Delisted (last: {last_price.strftime('%Y-%m-%d')})")
                                return
                        except:
                            print(f"⚠️  {ticker}: Delisted (Sharadar)")
                            return
                    
                    print(f"⚠️  {ticker}: Delisted (no Yahoo history)")
                    return
                
                # Sharadar says "active" but Yahoo has no data
                # This is VERY common - don't treat as unusual
                print(f"⚠️  {ticker}: Not tracked by Yahoo (OTC/SPAC/changed symbol)")
                return
        
        # Not found in metadata
        print(f"⚠️  {ticker}: Not in filtered ticker list")
    
    @staticmethod
    def download_multiple_tickers(
        tickers: List[str],
        start_date: str,
        end_date: str,
        ticker_metadata: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """Download historical data for multiple tickers with progress bar"""
        all_data = []
        error_stats = {
            'spac_units': 0,
            'warrants': 0,
            'rights': 0,
            'delisted_pre_2016': 0,
            'delisted_other': 0,
            'not_tracked_yahoo': 0,  # NEW: Sharadar has it, Yahoo doesn't
            'not_in_metadata': 0,     # NEW: Not found in filtered tickers
            'success': 0
        }
        
        if config.VERBOSE:
            print(f"💰 Downloading Yahoo Finance data for {len(tickers):,} tickers...")
        
        # Process in batches with progress bar
        with tqdm(total=len(tickers), disable=not config.VERBOSE) as pbar:
            for i in range(0, len(tickers), config.YAHOO_BATCH_SIZE):
                batch = tickers[i:i + config.YAHOO_BATCH_SIZE]
                
                for ticker in batch:
                    df = YahooFinanceClient.download_ticker_history(
                        ticker, start_date, end_date, ticker_metadata
                    )
                    if df is not None:
                        all_data.append(df)
                        error_stats['success'] += 1
                    else:
                        # Categorize failure - MUST MATCH _log_download_failure logic EXACTLY
                        
                        # 1. Check for suffix patterns
                        if '.' in ticker:
                            suffix = ticker.split('.')[-1]
                            if suffix in ['U', 'UN']:
                                error_stats['spac_units'] += 1
                            elif suffix in ['WS', 'WT', 'W']:
                                error_stats['warrants'] += 1
                            elif suffix in ['RT', 'R']:
                                error_stats['rights'] += 1
                            else:
                                error_stats['not_tracked_yahoo'] += 1  # Unknown suffix
                        
                        # 2. Check for ticker ending with U (SPAC unit)
                        elif ticker.endswith('U') and len(ticker) > 4:
                            error_stats['spac_units'] += 1
                        
                        # 3. Check for warrant/rights patterns
                        elif ticker.endswith(('WT', 'WS', 'RT')):
                            error_stats['warrants'] += 1
                        
                        # 4. Check metadata for delisting info
                        elif ticker_metadata is not None and ticker in ticker_metadata['ticker'].values:
                            ticker_info = ticker_metadata[ticker_metadata['ticker'] == ticker].iloc[0]
                            
                            # Check if Sharadar says it's delisted
                            if 'isdelisted' in ticker_info and ticker_info['isdelisted'] == 'Y':
                                # Delisted according to Sharadar
                                if 'lastpricedate' in ticker_info and pd.notna(ticker_info['lastpricedate']):
                                    try:
                                        last_price = pd.to_datetime(ticker_info['lastpricedate'])
                                        start = pd.to_datetime(start_date)
                                        if last_price < start:
                                            error_stats['delisted_pre_2016'] += 1
                                        else:
                                            error_stats['delisted_other'] += 1
                                    except:
                                        error_stats['delisted_other'] += 1
                                else:
                                    error_stats['delisted_other'] += 1
                            else:
                                # Sharadar says "active" (isdelisted = 'N') but Yahoo doesn't have it
                                # This is COMMON for OTC/SPAC/changed symbols
                                error_stats['not_tracked_yahoo'] += 1
                        
                        # 5. Not found in metadata at all
                        else:
                            error_stats['not_in_metadata'] += 1
                    
                    pbar.update(1)
                
                # Rate limiting between batches
                if i + config.YAHOO_BATCH_SIZE < len(tickers):
                    time.sleep(1)
        
        if not all_data:
            raise ValueError("No price data downloaded from Yahoo Finance")
        
        result = pd.concat(all_data, ignore_index=True)
        
        if config.VERBOSE:
            print(f"\n✅ Downloaded price data for {len(result['ticker'].unique()):,} tickers")
            print(f"\n📊 Download Statistics:")
            print(f"   Success: {error_stats['success']:,} ({error_stats['success']/len(tickers)*100:.1f}%)")
            print(f"   Failed - SPAC Units: {error_stats['spac_units']:,}")
            print(f"   Failed - Warrants/Rights: {error_stats['warrants'] + error_stats['rights']:,}")
            print(f"   Failed - Delisted pre-2016: {error_stats['delisted_pre_2016']:,}")
            print(f"   Failed - Delisted (other): {error_stats['delisted_other']:,}")
            print(f"   Failed - Not tracked by Yahoo: {error_stats['not_tracked_yahoo']:,}")
            print(f"   Failed - Not in metadata: {error_stats['not_in_metadata']:,}")
            total_failed = len(tickers) - error_stats['success']
            print(f"   Total Failed: {total_failed:,} ({total_failed/len(tickers)*100:.1f}%)")
        
        return result
