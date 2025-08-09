-- =================================================================
-- 1. Create the Sales Fact Table
-- =================================================================
CREATE TABLE [CG_Data_Warehouse].[stg].[cg_fact_sales]
(
    sales_id INT NOT NULL,
    order_number INT,
    product_code VARCHAR(20),
    customer_name VARCHAR(100),
    date_key INT,
    sales DECIMAL(12,2),
    quantity_ordered INT,
    price_each DECIMAL(10,2),
    deal_size VARCHAR(20),
    order_line_number INT
);
GO

-- =================================================================
-- 2. Insert Initial Data into the Sales Fact Table
-- =================================================================
INSERT INTO [CG_Data_Warehouse].[stg].[cg_fact_sales]
(
    sales_id, order_number, product_code, customer_name, date_key,
    sales, quantity_ordered, price_each, deal_size, order_line_number
)
SELECT
    ROW_NUMBER() OVER(ORDER BY s.ORDERNUMBER, s.ORDERLINENUMBER) AS sales_id,
    CAST(s.ORDERNUMBER AS INT),
    s.PRODUCTCODE,
    s.CUSTOMERNAME,
    -- This formula MUST match the one used for the date dimension
    CONVERT(INT, CONVERT(VARCHAR(8), CONVERT(DATETIME, s.ORDERDATE, 101), 112)) AS date_key,
    CAST(s.SALES AS DECIMAL(12,2)),
    CAST(s.QUANTITYORDERED AS INT),
    CAST(s.PRICEEACH AS DECIMAL(10,2)),
    s.DEALSIZE,
    CAST(s.ORDERLINENUMBER AS INT)
FROM
    [CG_Data_Warehouse].[stg].[sales_data_sample] s
WHERE
    s.ORDERNUMBER IS NOT NULL AND s.PRODUCTCODE IS NOT NULL AND s.CUSTOMERNAME IS NOT NULL;
GO

-- =================================================================
-- 3. Create the Stored Procedure to Load/Refresh Fact Data
-- This automates the process for future use.
-- =================================================================
CREATE PROC [stg].[stp_load_cg_fact_sales]
AS
BEGIN
    -- First, clear out all existing data from the fact table
    TRUNCATE TABLE [CG_Data_Warehouse].[stg].[cg_fact_sales];

    -- Next, insert the fresh data from the source table
    INSERT INTO [CG_Data_Warehouse].[stg].[cg_fact_sales]
    (
        sales_id, order_number, product_code, customer_name, date_key,
        sales, quantity_ordered, price_each, deal_size, order_line_number
    )
    SELECT
        ROW_NUMBER() OVER(ORDER BY s.ORDERNUMBER, s.ORDERLINENUMBER) AS sales_id,
        CAST(s.ORDERNUMBER AS INT),
        s.PRODUCTCODE,
        s.CUSTOMERNAME,
        CONVERT(INT, CONVERT(VARCHAR(8), CONVERT(DATETIME, s.ORDERDATE, 101), 112)) AS date_key,
        CAST(s.SALES AS DECIMAL(12,2)),
        CAST(s.QUANTITYORDERED AS INT),
        CAST(s.PRICEEACH AS DECIMAL(10,2)),
        s.DEALSIZE,
        CAST(s.ORDERLINENUMBER AS INT)
    FROM
        [CG_Data_Warehouse].[stg].[sales_data_sample] s
    WHERE
        s.ORDERNUMBER IS NOT NULL AND s.PRODUCTCODE IS NOT NULL AND s.CUSTOMERNAME IS NOT NULL;
END
GO

-- =================================================================
-- 4. Create a View for Easier Analysis
-- =================================================================
CREATE VIEW [stg].[vw_sales_analysis] AS
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
    [CG_Data_Warehouse].[stg].[cg_fact_sales] f
JOIN
    [CG_Data_Warehouse].[stg].[cg_dim_date] d ON f.date_key = d.date_key
JOIN
    [CG_Data_Warehouse].[stg].[cg_dim_product] p ON f.product_code = p.product_code
JOIN
    [CG_Data_Warehouse].[stg].[cg_dim_customer] c ON f.customer_name = c.customer_name
JOIN
    [CG_Data_Warehouse].[stg].[cg_dim_order] o ON f.order_number = o.order_number;
GO


select * from [CG_Data_Warehouse].[stg].[vw_sales_analysis]