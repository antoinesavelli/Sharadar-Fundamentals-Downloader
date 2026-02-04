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
                    # Rate limited - we hit 5,000 calls in 10 minutes (unlikely for TICKERS)
                    # OR account was already blocked from previous run
                    
                    if config.VERBOSE:
                        print(f"\n❌ RATE LIMITED (HTTP 429)")
                        print(f"   This means you hit 5,000 API calls in the last 10 minutes,")
                        print(f"   OR your account is still blocked from a previous violation.")
                        print(f"\n⏰ SOLUTION: Wait 10+ minutes before retrying.")
                        print(f"   Current time: {time.strftime('%H:%M:%S')}")
                        print(f"   Retry after: {time.strftime('%H:%M:%S', time.localtime(time.time() + 660))}")
                    
                    # Don't retry automatically - let user wait manually
                    raise Exception(
                        f"Rate limited. Wait 10+ minutes before retrying. "
                        f"If problem persists, contact clientsuccess@nasdaq.com"
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
                while status == "regenerating" and elapsed < config.EXPORT_MAX_WAIT:
                    if config.VERBOSE:
                        print(f"   Waiting for export... ({elapsed}s)")
                    time.sleep(config.EXPORT_POLL_INTERVAL)
                    elapsed += config.EXPORT_POLL_INTERVAL
                    
                    # Check status (doesn't count against rate limit)
                    check_response = requests.get(download_url)
                    if check_response.status_code == 200:
                        check_data = check_response.json()
                        status = check_data.get('file', {}).get('status', status)
                
                if status != "fresh":
                    raise Exception(f"Export timeout or failed. Status: {status}")
                
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
        """
        if config.VERBOSE:
            print("📊 Downloading SHARADAR/SF1 bulk export...")
            print(f"   Dimension: {config.DIMENSION}")
            print(f"   Indicators: {', '.join(config.INDICATORS)}")
        
        try:
            # Build filter parameters
            filters = {
                'dimension': config.DIMENSION,
            }
            
            # Add indicators filter (comma-separated list)
            if config.INDICATORS:
                filters['indicator'] = ','.join(config.INDICATORS)
            
            # Use bulk export with proper rate limiting
            df = self._download_bulk_export('SHARADAR/SF1', **filters)
            
            # Convert date column
            if 'datekey' in df.columns:
                df['date'] = pd.to_datetime(df['datekey'])
            
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
    
    @staticmethod
    def download_ticker_history(
        ticker: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """Download historical OHLCV data for a single ticker"""
        try:
            ticker_obj = yf.Ticker(ticker)
            hist = ticker_obj.history(start=start_date, end=end_date)
            
            if hist.empty:
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
            if config.VERBOSE:
                print(f"⚠️  Failed to download {ticker}: {e}")
            return None
    
    @staticmethod
    def download_multiple_tickers(
        tickers: List[str],
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """Download historical data for multiple tickers with progress bar"""
        all_data = []
        
        if config.VERBOSE:
            print(f"💰 Downloading Yahoo Finance data for {len(tickers):,} tickers...")
        
        # Process in batches with progress bar
        with tqdm(total=len(tickers), disable=not config.VERBOSE) as pbar:
            for i in range(0, len(tickers), config.YAHOO_BATCH_SIZE):
                batch = tickers[i:i + config.YAHOO_BATCH_SIZE]
                
                for ticker in batch:
                    df = YahooFinanceClient.download_ticker_history(
                        ticker, start_date, end_date
                    )
                    if df is not None:
                        all_data.append(df)
                    
                    pbar.update(1)
                
                # Rate limiting between batches
                if i + config.YAHOO_BATCH_SIZE < len(tickers):
                    time.sleep(1)
        
        if not all_data:
            raise ValueError("No price data downloaded from Yahoo Finance")
        
        result = pd.concat(all_data, ignore_index=True)
        
        if config.VERBOSE:
            print(f"✅ Downloaded price data for {len(result['ticker'].unique()):,} tickers")
        
        return result
