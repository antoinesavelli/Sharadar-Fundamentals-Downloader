"""
Data validation logic
"""
import pandas as pd
import config


class ValidationError(Exception):
    """Raised when validation fails"""
    pass


class DataValidator:
    """Validates dataset quality"""
    
    def validate_dataset(self, df: pd.DataFrame) -> dict:
        """Run all validations and return results"""
        warnings = []
        errors = []
        
        # Required columns
        required = ['ticker', 'date']
        missing = [col for col in required if col not in df.columns]
        if missing:
            errors.append(f"Missing required columns: {missing}")
        
        # Check for duplicates
        if df.duplicated(subset=['ticker', 'date']).any():
            dup_count = df.duplicated(subset=['ticker', 'date']).sum()
            errors.append(f"Found {dup_count:,} duplicate ticker-date pairs")
        
        # Check date range
        if 'date' in df.columns:
            date_range = f"{df['date'].min()} to {df['date'].max()}"
            warnings.append(f"Date range: {date_range}")
        
        # Skip price validation if Yahoo was skipped
        if config.SKIP_YAHOO_DOWNLOAD:
            warnings.append("Price data validation SKIPPED (Yahoo download disabled)")
            warnings.append("Market cap will be NULL until prices are added")
        else:
            # Check null percentages (only if we have prices)
            if 'market_cap' in df.columns:
                null_pct = df['market_cap'].isna().sum() / len(df)
                if null_pct > config.NULL_THRESHOLD_FAIL:
                    errors.append(f"Market cap nulls: {null_pct:.1%} (>{config.NULL_THRESHOLD_FAIL:.1%})")
                elif null_pct > config.NULL_THRESHOLD_WARN:
                    warnings.append(f"Market cap nulls: {null_pct:.1%} (>{config.NULL_THRESHOLD_WARN:.1%})")
        
        # Check ticker count
        ticker_count = df['ticker'].nunique()
        warnings.append(f"Unique tickers: {ticker_count:,}")
        
        return {
            'passed': len(errors) == 0,
            'warnings': warnings,
            'errors': errors
        }
