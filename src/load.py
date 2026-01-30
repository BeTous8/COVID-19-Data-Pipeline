"""
COVID-19 Data Loader - Supabase Edition
Loads transformed data into Supabase (PostgreSQL)
"""

import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

load_dotenv()

class CovidDataLoader:
    """Load transformed data into Supabase PostgreSQL database"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Connect to Supabase PostgreSQL database"""
        # Option 1: Use full connection string
        database_url = os.getenv('DATABASE_URL')
        
        if database_url:
            self.conn = psycopg2.connect(database_url)
        else:
            # Option 2: Use individual parameters
            self.conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                database=os.getenv('DB_NAME', 'postgres'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD'),
                port=os.getenv('DB_PORT', '5432')
            )
        
        self.cursor = self.conn.cursor()
        print("[OK] Connected to Supabase PostgreSQL")
    
    def create_run_metadata(self, data_source_url):
        """Create metadata record for this pipeline run"""
        try:
            self.cursor.execute("""
                INSERT INTO covid_metadata (data_source_url, status)
                VALUES (%s, %s)
                RETURNING run_id
            """, (data_source_url, 'IN_PROGRESS'))
            
            run_id = self.cursor.fetchone()[0]
            self.conn.commit()
            print(f"[OK] Created run metadata (run_id: {run_id})")
            return run_id
        except Exception as e:
            print(f"[ERROR] Error creating metadata: {str(e)}")
            self.conn.rollback()
            raise
    
    def load_data(self, df, run_id):
        """Bulk load data into covid_daily_cases table"""
        print(f"Loading {len(df):,} records into Supabase...")
        
        # Prepare data tuples
        data = [
            (
                row['country_region'],
                row['province_state'],
                row['date'],
                int(row['confirmed']),
                int(row['deaths']),
                int(row['recovered']),
                int(row['active']),
                run_id
            )
            for _, row in df.iterrows()
        ]
        
        try:
            # Bulk insert with ON CONFLICT handling (upsert)
            query = """
                INSERT INTO covid_daily_cases 
                (country_region, province_state, date, confirmed, deaths, recovered, active, run_id)
                VALUES %s
                ON CONFLICT (country_region, province_state, date) 
                DO UPDATE SET
                    confirmed = EXCLUDED.confirmed,
                    deaths = EXCLUDED.deaths,
                    recovered = EXCLUDED.recovered,
                    active = EXCLUDED.active,
                    run_id = EXCLUDED.run_id,
                    created_at = CURRENT_TIMESTAMP
            """
            
            # Load in batches to avoid memory issues
            batch_size = 1000
            total_loaded = 0
            
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                execute_values(self.cursor, query, batch, page_size=batch_size)
                total_loaded += len(batch)
                
                # Show progress
                if total_loaded % 10000 == 0:
                    print(f"  Loaded {total_loaded:,} / {len(data):,} records...")
            
            self.conn.commit()
            print(f"[OK] Successfully loaded {len(data):,} records to Supabase")
            return len(data)
            
        except Exception as e:
            print(f"[ERROR] Error loading data: {str(e)}")
            self.conn.rollback()
            raise
    
    def update_metadata(self, run_id, records_processed, status='SUCCESS', error_message=None):
        """Update metadata after load completes"""
        try:
            self.cursor.execute("""
                UPDATE covid_metadata
                SET records_processed = %s, 
                    status = %s,
                    error_message = %s
                WHERE run_id = %s
            """, (records_processed, status, error_message, run_id))
            self.conn.commit()
            print(f"[OK] Updated metadata (status: {status})")
        except Exception as e:
            print(f"[ERROR] Error updating metadata: {str(e)}")
            self.conn.rollback()
            raise
    
    def get_latest_stats(self):
        """Get summary statistics from the database"""
        try:
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT country_region) as countries,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date,
                    SUM(confirmed) as total_confirmed,
                    SUM(deaths) as total_deaths
                FROM covid_daily_cases
                WHERE date = (SELECT MAX(date) FROM covid_daily_cases)
            """)
            
            result = self.cursor.fetchone()
            return {
                'total_records': result[0],
                'countries': result[1],
                'earliest_date': result[2],
                'latest_date': result[3],
                'total_confirmed': result[4],
                'total_deaths': result[5]
            }
        except Exception as e:
            print(f"[WARN] Could not fetch stats: {str(e)}")
            return None
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("[OK] Database connection closed")

if __name__ == "__main__":
    # Test connection
    loader = CovidDataLoader()
    loader.connect()
    
    # Test metadata creation
    run_id = loader.create_run_metadata("https://github.com/CSSEGISandData/COVID-19")
    print(f"Test run_id: {run_id}")
    
    # Get stats (will be empty if tables just created)
    stats = loader.get_latest_stats()
    if stats:
        print(f"\nDatabase stats:")
        print(f"  Total records: {stats['total_records']:,}")
        print(f"  Countries: {stats['countries']}")
    
    loader.close()
