-- COVID-19 Data Pipeline - Sample Analytical Queries
-- Run these in Supabase SQL Editor to explore the data

-- ================================================
-- 1. Top 10 Countries by Total Confirmed Cases
-- ================================================
WITH latest_date AS (
    SELECT MAX(date) as max_date FROM covid_daily_cases
)
SELECT
    country_region,
    SUM(confirmed) as total_confirmed,
    SUM(deaths) as total_deaths,
    ROUND(SUM(deaths)::numeric / NULLIF(SUM(confirmed), 0) * 100, 2) as death_rate_pct
FROM covid_daily_cases
WHERE date = (SELECT max_date FROM latest_date)
GROUP BY country_region
ORDER BY total_confirmed DESC
LIMIT 10;

-- ================================================
-- 2. Global Daily Totals Trend (Last 30 Days)
-- ================================================
SELECT
    date,
    SUM(confirmed) as total_confirmed,
    SUM(deaths) as total_deaths,
    SUM(recovered) as total_recovered,
    SUM(active) as total_active
FROM covid_daily_cases
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY date
ORDER BY date DESC;

-- ================================================
-- 3. Daily New Cases (Calculated from Cumulative)
-- ================================================
WITH daily_totals AS (
    SELECT
        date,
        SUM(confirmed) as total_confirmed
    FROM covid_daily_cases
    GROUP BY date
)
SELECT
    date,
    total_confirmed,
    total_confirmed - LAG(total_confirmed) OVER (ORDER BY date) as new_cases
FROM daily_totals
ORDER BY date DESC
LIMIT 30;

-- ================================================
-- 4. Countries with Highest Death Rate
-- ================================================
WITH latest_date AS (
    SELECT MAX(date) as max_date FROM covid_daily_cases
)
SELECT
    country_region,
    SUM(confirmed) as total_confirmed,
    SUM(deaths) as total_deaths,
    ROUND(SUM(deaths)::numeric / NULLIF(SUM(confirmed), 0) * 100, 2) as death_rate_pct
FROM covid_daily_cases
WHERE date = (SELECT max_date FROM latest_date)
GROUP BY country_region
HAVING SUM(confirmed) > 10000  -- Filter for countries with significant cases
ORDER BY death_rate_pct DESC
LIMIT 10;

-- ================================================
-- 5. Monthly Aggregation by Country
-- ================================================
SELECT
    country_region,
    DATE_TRUNC('month', date) as month,
    MAX(confirmed) as confirmed_at_month_end,
    MAX(deaths) as deaths_at_month_end
FROM covid_daily_cases
WHERE country_region IN ('US', 'India', 'Brazil', 'France', 'Germany')
GROUP BY country_region, DATE_TRUNC('month', date)
ORDER BY country_region, month DESC;

-- ================================================
-- 6. Pipeline Run History
-- ================================================
SELECT
    run_id,
    run_date,
    records_processed,
    status,
    error_message
FROM covid_metadata
ORDER BY run_date DESC
LIMIT 10;

-- ================================================
-- 7. Data Freshness Check
-- ================================================
SELECT
    MAX(date) as latest_data_date,
    CURRENT_DATE - MAX(date) as days_behind,
    COUNT(DISTINCT country_region) as countries_tracked,
    COUNT(*) as total_records
FROM covid_daily_cases;
