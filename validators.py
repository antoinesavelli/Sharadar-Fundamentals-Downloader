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
        """
        Run all validation checks
        
        Returns:
            {
                'warnings': List[str],
                'errors': List[str],
                'passed': bool
            }
        """
        warnings = []
        errors = []
        
        # Check 1: Null percentages
        null_pct = df['market_cap'].isna().sum() / len(df)
        
        if null_pct > config.NULL_THRESHOLD_FAIL:
            errors.append(
                f"Market cap nulls: {null_pct:.2%} (exceeds {config.NULL_THRESHOLD_FAIL:.0%} threshold)"
            )
        elif null_pct > config.NULL_THRESHOLD_WARN:
            warnings.append(
                f"Market cap nulls: {null_pct:.2%} (exceeds {config.NULL_THRESHOLD_WARN:.0%} threshold)"
            )
        
        # Check 2: Negative or zero market caps (data errors)
        invalid_caps = df[
            (df['market_cap'].notna()) &
            (df['market_cap'] <= 0)
        ]
        
        if len(invalid_caps) > 0:
            errors.append(
                f"Found {len(invalid_caps):,} rows with market cap <= 0"
            )
            
            # Log top 10 examples
            for _, row in invalid_caps.head(10).iterrows():
                warnings.append(
                    f"  └─ {row['ticker']} on {row['date']}: "
                    f"SHARESBAS={row['SHARESBAS']:.2e}, Price=${row['open_price']:.2f}, "
                    f"MarketCap=${row['market_cap']:.2e}"
                )
        
        # Check 3: Stock splits detection (large SHARESBAS jumps)
        df_sorted = df.sort_values(['ticker', 'date'])
        df_sorted['sharesbas_pct_change'] = (
            df_sorted.groupby('ticker')['SHARESBAS'].pct_change()
        )
        
        splits = df_sorted[df_sorted['sharesbas_pct_change'].abs() > 0.5]
        
        if len(splits) > 0:
            warnings.append(
                f"Detected {len(splits):,} potential stock splits (>50% SHARESBAS change)"
            )
            
            # Verify market cap consistency across splits
            for _, row in splits.head(5).iterrows():
                warnings.append(
                    f"  └─ {row['ticker']} on {row['date']}: "
                    f"SHARESBAS change = {row['sharesbas_pct_change']:.1%}"
                )
        
        # Check 4: Missing ticker continuity (gaps >5 days)
        for ticker in df['ticker'].unique()[:100]:  # Sample first 100 tickers
            ticker_df = df[df['ticker'] == ticker].sort_values('date')
            
            if len(ticker_df) < 2:
                continue
            
            ticker_df['date_diff'] = (
                pd.to_datetime(ticker_df['date']).diff().dt.days
            )
            
            large_gaps = ticker_df[ticker_df['date_diff'] > 5]
            
            if len(large_gaps) > 0:
                warnings.append(
                    f"{ticker}: Found {len(large_gaps)} date gaps >5 days"
                )
        
        # Check 5: Date range coverage
        expected_start = pd.to_datetime(config.START_DATE).date()
        expected_end = pd.to_datetime(config.END_DATE).date()
        actual_start = df['date'].min()
        actual_end = df['date'].max()
        
        if actual_start > expected_start:
            warnings.append(
                f"Data starts at {actual_start}, expected {expected_start}"
            )
        
        if actual_end < expected_end:
            warnings.append(
                f"Data ends at {actual_end}, expected {expected_end}"
            )
        
        # Check 6: Verify expected columns exist
        required_cols = ['date', 'ticker', 'SHARESBAS', 'open_price', 'market_cap']
        missing_cols = set(required_cols) - set(df.columns)
        
        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")
        
        return {
            'warnings': warnings,
            'errors': errors,
            'passed': len(errors) == 0
        }
