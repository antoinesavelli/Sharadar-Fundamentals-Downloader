"""
Configuration file for Sharadar Downloader
Edit these parameters to customize your download
"""
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from keys.env file
load_dotenv('keys.env')

# API Configuration
NASDAQ_API_KEY = os.getenv('NASDAQ_API_KEY')
if not NASDAQ_API_KEY:
    raise ValueError("NASDAQ_API_KEY environment variable not set")

# Rate Limiting - Updated for Nasdaq bulk export limits
# Recommended for paid account
API_REQUEST_DELAY = 0.5  # 2 calls/sec = 1,200 calls per 10 min (24% of limit)

# Aggressive (still safe)
API_REQUEST_DELAY = 0.15  # ~6.6 calls/sec = 4,000 calls per 10 min (80% of limit)

MAX_RETRIES = 3

# Bulk Export Rate Limits (Nasdaq Premium)
BULK_EXPORT_LIMIT_PER_HOUR = 10  # Conservative limit for Table Exporter
BULK_EXPORT_COOLDOWN = 360  # 6 minutes between bulk exports (10 per hour = 1 every 6 min)
BULK_EXPORT_RETRY_DELAYS = [360, 600, 900]  # 6 min, 10 min, 15 min

# Async Export Settings
EXPORT_POLL_INTERVAL = 30  # Check export status every 30 seconds
EXPORT_MAX_WAIT = 300  # 5 minutes max wait for export to complete

# Date Range
START_DATE = "2016-01-01"
END_DATE = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')  # Yesterday

# Exchanges (US-based only)
EXCHANGES = [
    'NYSE',          # New York Stock Exchange
    'NASDAQ',        # NASDAQ
    'NYSEAMERICAN',  # American Stock Exchange (AMEX) - now called NYSE American
]

# Sharadar Parameters
DIMENSION = "ARQ"  # As-Reported Quarterly (best for backtesting)
INDICATORS = ["SHARESBAS"]  # Basic Shares Outstanding

# Price Source Configuration
SKIP_YAHOO_DOWNLOAD = True  # Set to True to skip Yahoo Finance (add prices from another API later)
PRICE_FIELD = "Open"  # Use open price to avoid forward-looking bias (when Yahoo is enabled)

# Directories
CACHE_DIR = "T:/fundamentals/cache"
OUTPUT_DIR = "T:/fundamentals/output"

# Validation Thresholds
NULL_THRESHOLD_WARN = 0.01  # Warn if >1% nulls
NULL_THRESHOLD_FAIL = 0.05  # Fail if >5% nulls (DISABLED when skipping Yahoo)

# Processing
VERBOSE = True
YAHOO_BATCH_SIZE = 50
MAX_RETRIES = 3  # Reduced
API_REQUEST_DELAY = 2.0  # Wait 2 seconds between API calls (PROACTIVE rate limiting)

# Progress Tracking
PROGRESS_FILE = os.path.join(CACHE_DIR, "progress.json")
VALIDATION_LOG = os.path.join(CACHE_DIR, "validation_log.txt")

# File Paths
TICKERS_FILE = os.path.join(CACHE_DIR, "tickers_universe.parquet")
SF1_RAW_FILE = os.path.join(CACHE_DIR, "sf1_raw.parquet")
YAHOO_PRICES_FILE = os.path.join(CACHE_DIR, "yahoo_prices.parquet")
MASTER_FILE = os.path.join(CACHE_DIR, "sf1_master.parquet")
