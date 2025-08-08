-- Date Dimension
CREATE TABLE cg_dim_date (
    date_key INT PRIMARY KEY,
    order_date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT
);

-- Product Dimension
CREATE TABLE cg_dim_product (
    product_code VARCHAR(20) PRIMARY KEY,
    product_line VARCHAR(50),
    msrp DECIMAL(10,2),
    product_name VARCHAR(100)
);

-- Customer Dimension (Corrected)
CREATE TABLE cg_dim_customer (
    customer_name VARCHAR(100) PRIMARY KEY,
    contact_first_name VARCHAR(50),
    contact_last_name VARCHAR(50),
    phone VARCHAR(30),
    addressline1 VARCHAR(100), -- Replaced 'address'
    addressline2 VARCHAR(100), -- Added
    city VARCHAR(50),
    state VARCHAR(20),
    postalcode VARCHAR(20),    -- Added
    country VARCHAR(50),
    territory VARCHAR(20)
);

-- Order Dimension
CREATE TABLE cg_dim_order (
    order_number INT PRIMARY KEY,
    status VARCHAR(20)
);

-- Sales Fact Table (Corrected)
CREATE TABLE cg_fact_sales (
    sales_id SERIAL PRIMARY KEY,
    order_number INT REFERENCES cg_dim_order(order_number),
    product_code VARCHAR(20) REFERENCES cg_dim_product(product_code),
    customer_name VARCHAR(100) REFERENCES cg_dim_customer(customer_name),
    date_key INT REFERENCES cg_dim_date(date_key),
    sales DECIMAL(12,2),
    quantity_ordered INT,
    price_each DECIMAL(10,2),
    deal_size VARCHAR(20),
    order_line_number INT -- Added
);

-- Create indexes for performance optimization
CREATE INDEX idx_sales_order_number ON cg_fact_sales(order_number);
CREATE INDEX idx_sales_product_code ON cg_fact_sales(product_code);
CREATE INDEX idx_sales_customer_name ON cg_fact_sales(customer_name);
CREATE INDEX idx_sales_date_key ON cg_fact_sales(date_key);

-- Create a view for easier analysis (Updated to include new columns)
CREATE VIEW vw_sales_analysis AS
SELECT
    d.year,
    d.month,
    d.day,
    p.product_line,
    c.customer_name,
    c.country,
    c.city,
    o.status,
    f.sales,
    f.quantity_ordered,
    f.price_each,
    f.deal_size,
    f.order_line_number
FROM
    cg_fact_sales f
JOIN
    cg_dim_date d ON f.date_key = d.date_key
JOIN
    cg_dim_product p ON f.product_code = p.product_code
JOIN
    cg_dim_customer c ON f.customer_name = c.customer_name
JOIN
    cg_dim_order o ON f.order_number = o.order_number;