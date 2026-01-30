"""
COVID-19 Data Transformer
Transforms wide-format time series data to long format and merges datasets
"""

import pandas as pd
from datetime import datetime

class CovidDataTransformer:
    """Transform COVID-19 data from wide to long format"""
    
    def __init__(self, raw_data_dir="data/raw"):
        self.raw_data_dir = raw_data_dir
    
    def load_raw_data(self, filename):
        """Load CSV file into pandas DataFrame"""
        filepath = f"{self.raw_data_dir}/{filename}"
        print(f"Loading {filename}...")
        return pd.read_csv(filepath)
    
    def pivot_to_long_format(self, df, value_name):
        """Convert wide format (dates as columns) to long format"""
        # Keep first 4 columns (Province/State, Country/Region, Lat, Long)
        id_vars = df.columns[:4].tolist()
        
        # Melt date columns into rows
        df_long = df.melt(
            id_vars=id_vars,
            var_name='date',
            value_name=value_name
        )
        
        # Convert date string to datetime
        df_long['date'] = pd.to_datetime(df_long['date'])
        
        return df_long
    
    def clean_data(self, df):
        """Clean and standardize data"""
        # Rename columns for consistency
        df = df.rename(columns={
            'Country/Region': 'country_region',
            'Province/State': 'province_state',
            'Lat': 'latitude',
            'Long': 'longitude'
        })
        
        # Fill NaN in province_state with 'All'
        df['province_state'] = df['province_state'].fillna('All')
        
        # Drop lat/long (not needed for analysis)
        df = df.drop(['latitude', 'longitude'], axis=1, errors='ignore')
        
        return df
    
    def merge_datasets(self, confirmed_df, deaths_df, recovered_df):
        """Merge confirmed, deaths, recovered into single dataset"""
        merge_keys = ['country_region', 'province_state', 'date']
        
        # Merge confirmed and deaths
        df = confirmed_df.merge(
            deaths_df, 
            on=merge_keys, 
            how='outer'
        )
        
        # Merge with recovered
        df = df.merge(
            recovered_df,
            on=merge_keys,
            how='outer'
        )
        
        # Fill NaN with 0
        df = df.fillna(0)
        
        # Calculate active cases
        df['active'] = df['confirmed'] - df['deaths'] - df['recovered']
        
        return df
    
    def transform_all(self):
        """Main transformation pipeline"""
        print("\n=== TRANSFORMING DATA ===")
        
        # Load raw data
        confirmed_raw = self.load_raw_data("time_series_covid19_confirmed_global.csv")
        deaths_raw = self.load_raw_data("time_series_covid19_deaths_global.csv")
        recovered_raw = self.load_raw_data("time_series_covid19_recovered_global.csv")
        
        # Transform to long format
        print("\nPivoting to long format...")
        confirmed = self.pivot_to_long_format(confirmed_raw, 'confirmed')
        deaths = self.pivot_to_long_format(deaths_raw, 'deaths')
        recovered = self.pivot_to_long_format(recovered_raw, 'recovered')
        
        # Clean data
        print("Cleaning data...")
        confirmed = self.clean_data(confirmed)
        deaths = self.clean_data(deaths)
        recovered = self.clean_data(recovered)
        
        # Merge datasets
        print("Merging datasets...")
        final_df = self.merge_datasets(confirmed, deaths, recovered)
        
        print(f"\n[OK] Transformed {len(final_df):,} records")
        print(f"  Date range: {final_df['date'].min()} to {final_df['date'].max()}")
        print(f"  Countries: {final_df['country_region'].nunique()}")
        
        return final_df

if __name__ == "__main__":
    transformer = CovidDataTransformer()
    df = transformer.transform_all()
    print("\nSample data:")
    print(df.head(10))
