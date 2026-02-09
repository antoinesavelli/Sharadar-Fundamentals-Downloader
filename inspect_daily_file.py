"""
Quick script to inspect daily parquet files
"""
import pandas as pd
from pathlib import Path

# File path
file_path = r"T:\trading\fundamentals\2020\01\2020-01-02.parquet"

# Check if file exists
if not Path(file_path).exists():
    print(f"❌ File not found: {file_path}")
    exit(1)

# Load first 50 rows
print(f"Loading first 50 rows from: {file_path}\n")
df = pd.read_parquet(file_path, engine='pyarrow')

# Display file info
print("="*80)
print("FILE INFORMATION")
print("="*80)
print(f"Total rows: {len(df):,}")
print(f"Total columns: {len(df.columns)}")
print(f"File size: {Path(file_path).stat().st_size / 1024 / 1024:.2f} MB")
print(f"\nColumns: {list(df.columns)}")

# Display first 50 rows
print("\n" + "="*80)
print("FIRST 50 ROWS")
print("="*80)
print(df.head(50).to_string())

# Additional statistics
print("\n" + "="*80)
print("DATA SUMMARY")
print("="*80)
print(df.head(50).describe())

# Check for nulls in first 50 rows
print("\n" + "="*80)
print("NULL VALUES (First 50 rows)")
print("="*80)
print(df.head(50).isnull().sum())

# Export first 50 rows to CSV
csv_output_path = r"T:\trading\fundamentals\sample_2020-01-02_first50.csv"
df.head(50).to_csv(csv_output_path, index=False)
print("\n" + "="*80)
print("CSV EXPORT")
print("="*80)
print(f"✅ First 50 rows exported to: {csv_output_path}")
print(f"   CSV file size: {Path(csv_output_path).stat().st_size / 1024:.2f} KB")