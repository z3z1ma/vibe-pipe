-- Initialize source database with sample customer data
-- This script is automatically run when the PostgreSQL container starts

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone VARCHAR(20),
    status VARCHAR(20) NOT NULL CHECK (status IN ('active', 'inactive', 'pending')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    total_orders INTEGER DEFAULT 0,
    total_spent NUMERIC(12, 2) DEFAULT 0.00,
    last_order_date DATE,
    country VARCHAR(100),
    city VARCHAR(100),
    postal_code VARCHAR(20)
);

-- Create index on updated_at for incremental loading
CREATE INDEX idx_customers_updated_at ON customers(updated_at);

-- Create index on status for filtering
CREATE INDEX idx_customers_status ON customers(status);

-- Create index on email for lookups
CREATE INDEX idx_customers_email ON customers(email);

-- Insert sample customer data
INSERT INTO customers (first_name, last_name, email, phone, status, created_at, updated_at, total_orders, total_spent, last_order_date, country, city, postal_code) VALUES
('John', 'Smith', 'john.smith@example.com', '+1-555-0101', 'active',
 '2024-01-15 10:30:00', '2024-01-28 14:25:00', 5, 529.95, '2024-01-25', 'USA', 'New York', '10001'),

('Jane', 'Doe', 'jane.doe@example.com', '+1-555-0102', 'active',
 '2024-01-16 11:00:00', '2024-01-27 16:40:00', 3, 239.97, '2024-01-26', 'USA', 'Los Angeles', '90001'),

('Bob', 'Johnson', 'bob.johnson@example.com', '+1-555-0103', 'inactive',
 '2023-12-01 09:15:00', '2024-01-20 10:00:00', 1, 49.99, '2023-12-10', 'USA', 'Chicago', '60601'),

('Alice', 'Williams', 'alice.williams@example.com', '+1-555-0104', 'active',
 '2024-01-10 14:20:00', '2024-01-26 09:30:00', 8, 849.92, '2024-01-24', 'USA', 'Houston', '77001'),

('Charlie', 'Brown', 'charlie.brown@example.com', '+1-555-0105', 'pending',
 '2024-01-25 16:00:00', '2024-01-25 16:00:00', 0, 0.00, NULL, 'USA', 'Phoenix', '85001'),

('Diana', 'Miller', 'diana.miller@example.com', '+1-555-0106', 'active',
 '2024-01-05 08:45:00', '2024-01-27 11:15:00', 12, 1259.88, '2024-01-27', 'USA', 'Philadelphia', '19101'),

('Edward', 'Davis', 'edward.davis@example.com', NULL, 'active',
 '2024-01-12 13:30:00', '2024-01-26 15:50:00', 2, 179.98, '2024-01-22', 'USA', 'San Antonio', '78201'),

('Fiona', 'Garcia', 'fiona.garcia@example.com', '+1-555-0108', 'inactive',
 '2023-11-20 10:00:00', '2024-01-15 09:20:00', 0, 0.00, NULL, 'USA', 'San Diego', '92101'),

('George', 'Martinez', 'george.martinez@example.com', '+1-555-0109', 'active',
 '2024-01-18 11:45:00', '2024-01-28 12:00:00', 6, 629.94, '2024-01-28', 'USA', 'Dallas', '75201'),

('Hannah', 'Anderson', 'hannah.anderson@example.com', '+1-555-0110', 'active',
 '2024-01-08 09:00:00', '2024-01-25 14:30:00', 4, 399.96, '2024-01-23', 'USA', 'San Jose', '95101'),

('Isaac', 'Taylor', 'isaac.taylor@example.com', NULL, 'pending',
 '2024-01-26 15:20:00', '2024-01-26 15:20:00', 0, 0.00, NULL, 'USA', 'Austin', '78701'),

('Julia', 'Thomas', 'julia.thomas@example.com', '+1-555-0112', 'active',
 '2024-01-14 12:15:00', '2024-01-27 10:45:00', 7, 749.93, '2024-01-26', 'USA', 'Jacksonville', '32201'),

('Kevin', 'Hernandez', 'kevin.hernandez@example.com', '+1-555-0113', 'inactive',
 '2023-12-15 14:30:00', '2024-01-10 11:00:00', 2, 129.98, '2024-01-05', 'USA', 'Fort Worth', '76101'),

('Laura', 'Moore', 'laura.moore@example.com', '+1-555-0114', 'active',
 '2024-01-20 10:00:00', '2024-01-28 13:20:00', 3, 289.97, '2024-01-27', 'USA', 'Columbus', '43201'),

('Michael', 'Martin', 'michael.martin@example.com', '+1-555-0115', 'active',
 '2024-01-02 08:00:00', '2024-01-26 16:00:00', 10, 1099.90, '2024-01-25', 'USA', 'Charlotte', '28201'),

('Nancy', 'Jackson', 'nancy.jackson@example.com', NULL, 'active',
 '2024-01-11 11:30:00', '2024-01-25 09:15:00', 1, 89.99, '2024-01-24', 'USA', 'San Francisco', '94102'),

('Oscar', 'Thompson', 'oscar.thompson@example.com', '+1-555-0117', 'pending',
 '2024-01-27 14:00:00', '2024-01-27 14:00:00', 0, 0.00, NULL, 'USA', 'Indianapolis', '46201'),

('Patricia', 'White', 'patricia.white@example.com', '+1-555-0118', 'active',
 '2024-01-07 10:45:00', '2024-01-28 11:30:00', 9, 929.91, '2024-01-28', 'USA', 'Seattle', '98101'),

('Quinn', 'Lopez', 'quinn.lopez@example.com', '+1-555-0119', 'inactive',
 '2023-12-10 13:00:00', '2024-01-12 15:45:00', 0, 0.00, NULL, 'USA', 'Denver', '80201'),

('Rachel', 'Lee', 'rachel.lee@example.com', '+1-555-0120', 'active',
 '2024-01-22 09:30:00', '2024-01-27 12:15:00', 5, 549.95, '2024-01-26', 'USA', 'Washington', '20001'),

('Samuel', 'Gonzalez', 'samuel.gonzalez@example.com', NULL, 'active',
 '2024-01-09 08:15:00', '2024-01-26 14:00:00', 4, 379.96, '2024-01-25', 'USA', 'Boston', '2101'),

('Tina', 'Harris', 'tina.harris@example.com', '+1-555-0122', 'active',
 '2024-01-16 12:00:00', '2024-01-28 15:00:00', 6, 629.94, '2024-01-27', 'USA', 'El Paso', '79901'),

('Victor', 'Clark', 'victor.clark@example.com', '+1-555-0123', 'pending',
 '2024-01-27 10:30:00', '2024-01-27 10:30:00', 0, 0.00, NULL, 'USA', 'Detroit', '48201'),

('Wendy', 'Lewis', 'wendy.lewis@example.com', '+1-555-0124', 'active',
 '2024-01-04 11:00:00', '2024-01-25 13:45:00', 3, 299.97, '2024-01-24', 'USA', 'Nashville', '37201'),

('Xavier', 'Robinson', 'xavier.robinson@example.com', NULL, 'inactive',
 '2023-11-25 09:00:00', '2024-01-08 10:30:00', 1, 59.99, '2024-01-05', 'USA', 'Portland', '97201'),

('Yvonne', 'Walker', 'yvonne.walker@example.com', '+1-555-0126', 'active',
 '2024-01-13 14:15:00', '2024-01-27 16:30:00', 8, 839.92, '2024-01-26', 'USA', 'Oklahoma City', '73101'),

('Zachary', 'Young', 'zachary.young@example.com', '+1-555-0127', 'active',
 '2024-01-19 10:00:00', '2024-01-28 12:00:00', 11, 1149.89, '2024-01-27', 'USA', 'Las Vegas', '89101');

-- Create a function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to auto-update updated_at
CREATE TRIGGER update_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions to ETL user
GRANT SELECT ON customers TO etl_user;

-- Verify data
SELECT COUNT(*) as customer_count FROM customers;
SELECT status, COUNT(*) as count FROM customers GROUP BY status;
