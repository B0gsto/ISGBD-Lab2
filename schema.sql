-- E-commerce schema for Query Optimizer Lab
-- Run this in pgAdmin to set up the database

-- Drop tables if they exist (for clean restart)
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS categories CASCADE;

-- Create tables
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT
);

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category_id INTEGER REFERENCES categories(id),
    price DECIMAL(10,2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    order_date TIMESTAMP DEFAULT NOW(),
    total DECIMAL(12,2),
    status VARCHAR(20) DEFAULT 'pending'
);

CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL
);

-- Create indexes (3 required, 1 composite)
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_orderitems_order_product ON order_items(order_id, product_id);  -- Composite index

-- Insert sample data for realistic statistics

-- Categories (100 rows)
INSERT INTO categories (name, description)
SELECT 
    'Category ' || i,
    'Description for category ' || i
FROM generate_series(1, 100) AS i;

-- Customers (10,000 rows)
INSERT INTO customers (name, email, country)
SELECT 
    'Customer ' || i,
    'customer' || i || '@example.com',
    CASE (i % 10)
        WHEN 0 THEN 'USA'
        WHEN 1 THEN 'UK'
        WHEN 2 THEN 'Germany'
        WHEN 3 THEN 'France'
        WHEN 4 THEN 'Spain'
        WHEN 5 THEN 'Italy'
        WHEN 6 THEN 'Canada'
        WHEN 7 THEN 'Australia'
        WHEN 8 THEN 'Japan'
        ELSE 'Brazil'
    END
FROM generate_series(1, 10000) AS i;

-- Products (5,000 rows)
INSERT INTO products (name, category_id, price, stock_quantity)
SELECT 
    'Product ' || i,
    (i % 100) + 1,  -- Links to categories 1-100
    ROUND((RANDOM() * 999 + 1)::numeric, 2),
    FLOOR(RANDOM() * 1000)::integer
FROM generate_series(1, 5000) AS i;

-- Orders (50,000 rows)
INSERT INTO orders (customer_id, order_date, total, status)
SELECT 
    (i % 10000) + 1,  -- Links to customers 1-10000
    NOW() - (RANDOM() * 365 || ' days')::interval,
    ROUND((RANDOM() * 1000 + 10)::numeric, 2),
    CASE (i % 4)
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'shipped'
        WHEN 2 THEN 'delivered'
        ELSE 'cancelled'
    END
FROM generate_series(1, 50000) AS i;

-- Order Items (150,000 rows)
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT 
    (i % 50000) + 1,  -- Links to orders 1-50000
    (i % 5000) + 1,    -- Links to products 1-5000
    FLOOR(RANDOM() * 10 + 1)::integer,
    ROUND((RANDOM() * 100 + 5)::numeric, 2)
FROM generate_series(1, 150000) AS i;

-- Update statistics for cost estimation
ANALYZE;

-- Verify data counts
SELECT 'categories' AS table_name, COUNT(*) AS row_count FROM categories
UNION ALL
SELECT 'customers', COUNT(*) FROM customers
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL
SELECT 'products', COUNT(*) FROM products;
