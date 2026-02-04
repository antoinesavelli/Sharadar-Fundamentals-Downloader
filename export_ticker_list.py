"""
Standalone script to export ticker universe to CSV
"""
import pandas as pd
from pathlib import Path
import config
from client import SharadarClient


def export_tickers():
    """Download and export ticker list to CSV"""
    print("📋 Exporting Sharadar ticker universe...")
    
    # Initialize client
    client = SharadarClient(config.NASDAQ_API_KEY)
    
    # Download tickers table
    tickers = client.download_tickers_table()
    
    # Filter for US exchanges
    us_tickers = tickers[
        (tickers['exchange'].isin(config.EXCHANGES)) &
        (tickers['table'] == 'SF1')
    ]
    
    # Select relevant columns
    export_df = us_tickers[[
        'ticker',
        'name',
        'exchange',
        'isdelisted',
        'category',
        'sector',
        'industry',
        'scalemarketcap',
        'firstpricedate',
        'lastpricedate'
    ]].copy()
    
    # Save to CSV in output directory
    Path(config.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    output_file = Path(config.OUTPUT_DIR) / "tickers_universe.csv"
    export_df.to_csv(output_file, index=False)
    
    print(f"✅ Exported {len(export_df):,} tickers to {output_file}")
    print(f"   Active: {(export_df['isdelisted'] == 'N').sum():,}")
    print(f"   Delisted: {(export_df['isdelisted'] == 'Y').sum():,}")
    
    # Download and export indicator metadata
    print("\n📖 Exporting indicator metadata...")
    indicators = client.download_indicators_metadata()
    metadata_file = Path(config.OUTPUT_DIR) / "indicators_metadata.csv"
    indicators.to_csv(metadata_file, index=False)
    print(f"✅ Exported {len(indicators):,} indicators to {metadata_file}")


if __name__ == "__main__":
    export_tickers()
