"""
COVID-19 Data Quality Validator
Validates transformed data before loading into database
"""

import pandas as pd
from datetime import datetime


class DataQualityValidator:
    """Validate COVID-19 data quality before loading"""

    def __init__(self, df):
        self.df = df
        self.errors = []
        self.warnings = []

    def check_null_values(self):
        """Check for NULL values in critical columns"""
        critical_columns = ['country_region', 'date', 'confirmed', 'deaths']
        for col in critical_columns:
            null_count = self.df[col].isnull().sum()
            if null_count > 0:
                self.errors.append(f"Found {null_count} NULL values in {col}")
        return len(self.errors) == 0

    def check_negative_values(self):
        """Check for negative values in numeric columns"""
        numeric_columns = ['confirmed', 'deaths', 'recovered']
        for col in numeric_columns:
            negative_count = (self.df[col] < 0).sum()
            if negative_count > 0:
                self.warnings.append(f"Found {negative_count} negative values in {col}")
        return True  # Warnings don't fail validation

    def check_date_range(self):
        """Check that dates are within valid range"""
        min_date = pd.Timestamp('2020-01-01')
        max_date = pd.Timestamp.now()

        invalid_dates = self.df[
            (self.df['date'] < min_date) | (self.df['date'] > max_date)
        ]

        if len(invalid_dates) > 0:
            self.errors.append(f"Found {len(invalid_dates)} records with invalid dates")
        return len(invalid_dates) == 0

    def check_logical_consistency(self):
        """Check that active cases calculation is consistent"""
        expected_active = self.df['confirmed'] - self.df['deaths'] - self.df['recovered']
        inconsistent = (self.df['active'] != expected_active).sum()

        if inconsistent > 0:
            self.errors.append(f"Found {inconsistent} records with inconsistent active calculation")
        return inconsistent == 0

    def validate_all(self):
        """Run all validation checks"""
        print("\n=== DATA QUALITY CHECKS ===")

        checks = [
            ("NULL values check", self.check_null_values),
            ("Negative values check", self.check_negative_values),
            ("Date range check", self.check_date_range),
            ("Logical consistency check", self.check_logical_consistency),
        ]

        all_passed = True
        for name, check_func in checks:
            passed = check_func()
            status = "[PASS]" if passed else "[FAIL]"
            print(f"  {status} {name}")
            if not passed:
                all_passed = False

        # Print warnings
        for warning in self.warnings:
            print(f"  [WARN] Warning: {warning}")

        # Print errors
        for error in self.errors:
            print(f"  [FAIL] Error: {error}")

        if all_passed:
            print("\n[SUCCESS] All data quality checks passed!")
        else:
            print("\n[FAILED] Data quality checks failed!")

        return all_passed, self.errors, self.warnings


def validate_data(df):
    """Convenience function to validate a DataFrame"""
    validator = DataQualityValidator(df)
    return validator.validate_all()


if __name__ == "__main__":
    # Test with sample data
    from transform import CovidDataTransformer

    transformer = CovidDataTransformer()
    df = transformer.transform_all()

    passed, errors, warnings = validate_data(df)
