-- COVID-19 Data Pipeline Database Schema

-- Drop existing tables
DROP TABLE IF EXISTS covid_daily_cases CASCADE;
DROP TABLE IF EXISTS covid_metadata CASCADE;

-- Metadata table to track pipeline runs
CREATE TABLE covid_metadata (
    run_id SERIAL PRIMARY KEY,
    run_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_processed INTEGER,
    data_source_url VARCHAR(500),
    status VARCHAR(50),
    error_message TEXT
);

-- Main data table for daily COVID-19 cases
CREATE TABLE covid_daily_cases (
    id SERIAL PRIMARY KEY,
    country_region VARCHAR(100) NOT NULL,
    province_state VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    confirmed INTEGER DEFAULT 0,
    deaths INTEGER DEFAULT 0,
    recovered INTEGER DEFAULT 0,
    active INTEGER DEFAULT 0,
    run_id INTEGER REFERENCES covid_metadata(run_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_location_date UNIQUE(country_region, province_state, date)
);

-- Indexes for query performance
CREATE INDEX idx_country_date ON covid_daily_cases(country_region, date);
CREATE INDEX idx_date ON covid_daily_cases(date);
CREATE INDEX idx_run_id ON covid_daily_cases(run_id);

-- Verify tables created
SELECT 'Tables created successfully!' as status;
