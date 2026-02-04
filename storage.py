"""
Storage utilities for Parquet files
"""
import pandas as pd
from pathlib import Path
import config


class ParquetStorage:
    """Handle Parquet file operations"""
    
    @staticmethod
    def save_parquet(df: pd.DataFrame, filepath: str):
        """Save DataFrame to Parquet with compression"""
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        
        df.to_parquet(
            filepath,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        if config.VERBOSE:
            size_mb = Path(filepath).stat().st_size / 1024 / 1024
            print(f"💾 Saved: {filepath} ({size_mb:.1f} MB)")
    
    @staticmethod
    def load_parquet(filepath: str) -> pd.DataFrame:
        """Load DataFrame from Parquet"""
        return pd.read_parquet(filepath, engine='pyarrow')
