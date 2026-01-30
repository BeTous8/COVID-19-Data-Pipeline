"""
COVID-19 Data Extractor
Downloads time-series data from Johns Hopkins CSSE GitHub repository
"""

import requests
import pandas as pd
from datetime import datetime
import os

class CovidDataExtractor:
    """Extract COVID-19 time series data from Johns Hopkins CSSE"""
    
    BASE_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/"
    
    def __init__(self, output_dir="data/raw"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def download_file(self, filename):
        """Download a specific CSV file from Johns Hopkins"""
        url = f"{self.BASE_URL}{filename}"
        print(f"Downloading {filename}...")
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            filepath = os.path.join(self.output_dir, filename)
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            print(f"[OK] Downloaded to {filepath}")
            return filepath
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Error downloading {filename}: {str(e)}")
            raise
    
    def extract_all(self):
        """Download all time series files"""
        files = [
            "time_series_covid19_confirmed_global.csv",
            "time_series_covid19_deaths_global.csv",
            "time_series_covid19_recovered_global.csv"
        ]
        
        downloaded_files = {}
        for filename in files:
            downloaded_files[filename] = self.download_file(filename)
        
        return downloaded_files

if __name__ == "__main__":
    extractor = CovidDataExtractor()
    files = extractor.extract_all()
    print(f"\n[SUCCESS] Extracted {len(files)} files successfully")
