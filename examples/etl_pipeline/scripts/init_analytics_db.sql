-- Initialize analytics database with target tables
-- This script is automatically run when the analytics PostgreSQL container starts

-- Create analytics schema
CREATE SCHEMA IF NOT EXISTS analytics;

-- Create customers_analytics table for aggregated data
CREATE TABLE IF NOT EXISTS analytics.customers_analytics (
    customer_id INTEGER PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL,
    customer_tenure_days INTEGER NOT NULL,
    is_active BOOLEAN NOT NULL,
    total_orders INTEGER NOT NULL,
    total_spent NUMERIC(12, 2) NOT NULL,
    avg_order_value NUMERIC(12, 2) NOT NULL,
    has_ordered BOOLEAN NOT NULL,
    days_since_last_order INTEGER,
    country VARCHAR(100),
    acquisition_year INTEGER NOT NULL,
    acquisition_month VARCHAR(10) NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for analytics queries
CREATE INDEX idx_analytics_customers_status ON analytics.customers_analytics(status);
CREATE INDEX idx_analytics_customers_country ON analytics.customers_analytics(country);
CREATE INDEX idx_analytics_customers_acquisition ON analytics.customers_analytics(acquisition_year, acquisition_month);
CREATE INDEX idx_analytics_customers_tenure ON analytics.customers_analytics(customer_tenure_days);

-- Create materialized view for daily summary
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.daily_customer_summary AS
SELECT
    acquisition_year,
    acquisition_month,
    status,
    COUNT(*) as customer_count,
    SUM(total_spent) as total_revenue,
    AVG(total_spent) as avg_revenue_per_customer,
    SUM(total_orders) as total_orders,
    AVG(customer_tenure_days) as avg_tenure_days
FROM analytics.customers_analytics
GROUP BY acquisition_year, acquisition_month, status;

-- Create unique index for materialized view refresh
CREATE UNIQUE INDEX idx_daily_customer_summary_unique
ON analytics.daily_customer_summary(acquisition_year, acquisition_month, status);

-- Grant permissions to analytics user
GRANT USAGE ON SCHEMA analytics TO analytics_user;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analytics_user;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA analytics TO analytics_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO analytics_user;

-- Create refresh function for materialized view
CREATE OR REPLACE FUNCTION analytics.refresh_daily_summary()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.daily_customer_summary;
END;
$$ LANGUAGE plpgsql;

-- Grant execute on refresh function
GRANT EXECUTE ON FUNCTION analytics.refresh_daily_summary() TO analytics_user;

COMMENT ON TABLE analytics.customers_analytics IS 'Aggregated customer data for analytics and reporting';
COMMENT ON MATERIALIZED VIEW analytics.daily_customer_summary IS 'Daily summary of customer metrics by acquisition period and status';
