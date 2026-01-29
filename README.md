# COVID-19 Data Pipeline

ETL pipeline extracting COVID-19 time-series data from Johns Hopkins CSSE, transforming with Pandas, and loading into PostgreSQL with data quality validations.

## 🎯 Project Overview

This project demonstrates end-to-end data engineering skills by building a production-grade ETL pipeline that:
- **Extracts** daily COVID-19 data from Johns Hopkins GitHub repository
- **Transforms** wide-format time series to long format using Pandas
- **Loads** data into PostgreSQL with upsert handling
- **Validates** data quality (nulls, negatives, duplicates, date ranges)
- **Handles** idempotency (safe to run multiple times)

## 🛠️ Technologies

- **Python 3.9+** - Core programming language
- **Pandas** - Data transformation and analysis
- **Supabase (PostgreSQL)** - Cloud-hosted database
- **psycopg2** - PostgreSQL adapter for Python
- **Requests** - HTTP library for API calls
- **python-dotenv** - Environment variable management

## 📦 Installation

### 1. Supabase Setup

This project uses **Supabase** (hosted PostgreSQL) instead of local installation.

**If you already have a Supabase project (like from Memora):**
- ✓ You're ready to go! Just get your connection string (Step 5 below)

**If you need a new Supabase project:**
1. Go to [supabase.com](https://supabase.com)
2. Sign in (or create free account)
3. Click "New Project"
4. Choose organization and set project name
5. **Save your database password** (you'll need it!)
6. Wait ~2 minutes for project to initialize

### 2. Clone or Setup Project

```bash
# Create project directory
mkdir C:\data-engineering-projects
cd C:\data-engineering-projects

# If cloning from GitHub:
git clone https://github.com/BeTous8/covid-data-pipeline.git
cd covid-data-pipeline

# Or if setting up manually:
# Just extract the downloaded files here
```

### 3. Create Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (Mac/Linux)
source venv/bin/activate
```

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

### 5. Get Supabase Connection String

1. Open your Supabase project dashboard
2. Click **Project Settings** (gear icon in sidebar)
3. Click **Database** 
4. Scroll to **Connection string** section
5. Select **URI** tab
6. Copy the connection string (looks like `postgresql://postgres:[YOUR-PASSWORD]@db.xxx.supabase.co:5432/postgres`)
7. Replace `[YOUR-PASSWORD]` with your actual database password

### 6. Configure Database Connection

Create `.env` file in project root:

```bash
DATABASE_URL=postgresql://postgres:your_actual_password@db.xxx.supabase.co:5432/postgres
```

**Important:** Replace with your actual Supabase connection details!

### 7. Initialize Database Tables

**Option A: Using Supabase SQL Editor (Recommended)**
1. Open Supabase Dashboard
2. Click **SQL Editor** in sidebar
3. Click **New Query**
4. Copy contents of `sql/create_tables.sql` and paste
5. Click **Run** button

**Option B: Using psql command line**
```bash
psql "postgresql://postgres:your_password@db.xxx.supabase.co:5432/postgres" -f sql/create_tables.sql
```

## 🚀 Usage

### Run Complete Pipeline

```bash
python src/main.py
```

Expected output:
```
==================================================
COVID-19 DATA PIPELINE - STARTING
==================================================

[1/4] EXTRACTING DATA...
Downloading time_series_covid19_confirmed_global.csv...
✓ Downloaded to data/raw/time_series_covid19_confirmed_global.csv
...

[2/4] TRANSFORMING DATA...
✓ Transformed 1,234,567 records

[3/4] RUNNING DATA QUALITY CHECKS...
✓ No NULL values in critical columns
✓ No negative values in numeric columns
✓ Date range is valid
✓ No duplicate records
✅ All data quality checks passed!

[4/4] LOADING INTO DATABASE...
✓ Loaded 1,234,567 records

==================================================
✅ PIPELINE COMPLETED SUCCESSFULLY
   Records processed: 1,234,567
   Run ID: 1
==================================================
```

### Run Individual Modules

```bash
# Test extraction only
python src/extract.py

# Test transformation only
python src/transform.py
```

## 📊 Sample Queries

See `sql/queries.sql` for analytical queries:

```sql
-- Top 10 countries by confirmed cases
WITH latest_date AS (
    SELECT MAX(date) as max_date FROM covid_daily_cases
)
SELECT 
    country_region,
    SUM(confirmed) as total_confirmed,
    SUM(deaths) as total_deaths
FROM covid_daily_cases
WHERE date = (SELECT max_date FROM latest_date)
GROUP BY country_region
ORDER BY total_confirmed DESC
LIMIT 10;
```

## ✅ Data Quality Checks

Pipeline validates:
- ✓ No NULL values in critical columns (country, date)
- ✓ No negative case counts
- ✓ Date ranges are valid (2020+)
- ✓ No duplicate records per location/date

## 📁 Project Structure

```
covid-data-pipeline/
├── src/
│   ├── extract.py          # Download data from Johns Hopkins
│   ├── transform.py        # Transform and clean data
│   ├── load.py             # Load into PostgreSQL
│   ├── data_quality.py     # Validation checks
│   └── main.py             # Pipeline orchestration
├── sql/
│   ├── create_tables.sql   # Database schema
│   └── queries.sql         # Sample analytical queries
├── data/                   # Local data files (gitignored)
├── requirements.txt        # Python dependencies
├── .env.example            # Environment variable template
└── README.md              # This file
```

## 🎯 Future Enhancements

- [ ] Add Apache Airflow for scheduling
- [ ] Implement incremental loads (only new data)
- [ ] Add visualization layer (Tableau/Plotly)
- [ ] Deploy to cloud (AWS/Azure)
- [ ] Add unit tests with pytest
- [ ] Implement CDC (Change Data Capture)

## 👤 Author

**Benjamin Tousifar**
- GitHub: [@BeTous8](https://github.com/BeTous8)
- LinkedIn: [benjamin-tousifar](https://www.linkedin.com/in/benjamin-tousifar-44054a112/)
- Email: Available on request

## 📝 License

This project is open source and available for educational purposes.

## 🙏 Data Source

Data provided by Johns Hopkins University CSSE:
https://github.com/CSSEGISandData/COVID-19
