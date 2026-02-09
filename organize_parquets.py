"""
Reorganize daily parquet files into year/month folder structure
"""
from pathlib import Path
from datetime import datetime
import shutil
import config


def organize_daily_parquets():
    """Reorganize flat daily parquets into YYYY/MM/ structure"""
    
    output_dir = Path(config.OUTPUT_DIR)
    
    # Find all date-formatted parquet files (YYYY-MM-DD.parquet)
    parquet_files = list(output_dir.glob("????-??-??.parquet"))
    
    if not parquet_files:
        print("❌ No daily parquet files found in output directory")
        return
    
    print(f"📂 Found {len(parquet_files):,} daily parquet files")
    print(f"🔄 Organizing into year/month structure...\n")
    
    moved_count = 0
    skipped_count = 0
    
    for file_path in parquet_files:
        try:
            # Parse date from filename (YYYY-MM-DD.parquet)
            date_str = file_path.stem  # Gets filename without extension
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            
            # Create year/month directory structure
            year_dir = output_dir / str(date_obj.year)
            month_dir = year_dir / f"{date_obj.month:02d}"
            month_dir.mkdir(parents=True, exist_ok=True)
            
            # Define new file path
            new_path = month_dir / file_path.name
            
            # Move file
            if not new_path.exists():
                shutil.move(str(file_path), str(new_path))
                moved_count += 1
                
                if config.VERBOSE and moved_count % 100 == 0:
                    print(f"  Moved {moved_count:,} files...")
            else:
                skipped_count += 1
                print(f"  ⚠️  Skipped {file_path.name} (already exists)")
        
        except Exception as e:
            print(f"  ❌ Error processing {file_path.name}: {e}")
    
    print(f"\n✅ Organization complete!")
    print(f"   Files moved: {moved_count:,}")
    if skipped_count > 0:
        print(f"   Files skipped: {skipped_count:,}")
    
    # Show directory structure summary
    print(f"\n📊 Directory structure:")
    for year_dir in sorted(output_dir.glob("[0-9][0-9][0-9][0-9]")):
        month_count = len(list(year_dir.glob("[0-9][0-9]")))
        file_count = len(list(year_dir.glob("*/*.parquet")))
        print(f"   {year_dir.name}/: {month_count} months, {file_count:,} files")


if __name__ == "__main__":
    print("\n" + "="*60)
    print("ORGANIZE DAILY PARQUETS - YEAR/MONTH STRUCTURE")
    print("="*60 + "\n")
    
    organize_daily_parquets()
    
    print("\n" + "="*60 + "\n")
