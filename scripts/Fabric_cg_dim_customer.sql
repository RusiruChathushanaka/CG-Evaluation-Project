-- =================================================================
-- 1. Create the Customer Dimension Table
-- =================================================================
CREATE TABLE [CG_Data_Warehouse].[stg].[cg_dim_customer]
(
    customer_name VARCHAR(100) NOT NULL,
    contact_first_name VARCHAR(50),
    contact_last_name VARCHAR(50),
    phone VARCHAR(30),
    addressline1 VARCHAR(100),
    addressline2 VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(20),
    postalcode VARCHAR(20),
    country VARCHAR(50),
    territory VARCHAR(20)
);
GO

-- =================================================================
-- 2. Insert Initial Data into the Customer Dimension Table
-- =================================================================
INSERT INTO [CG_Data_Warehouse].[stg].[cg_dim_customer]
(
    customer_name, contact_first_name, contact_last_name, phone,
    addressline1, addressline2, city, state, postalcode, country, territory
)
SELECT DISTINCT
    CUSTOMERNAME,
    CONTACTFIRSTNAME,
    CONTACTLASTNAME,
    PHONE,
    ADDRESSLINE1,
    ADDRESSLINE2,
    CITY,
    STATE,
    POSTALCODE,
    COUNTRY,
    TERRITORY
FROM
    [CG_Data_Warehouse].[stg].[sales_data_sample]
WHERE
    CUSTOMERNAME IS NOT NULL;
GO

-- =================================================================
-- 3. Create the Stored Procedure to Load/Refresh Customer Data
-- This automates the process for future use.
-- =================================================================
CREATE PROC [stg].[stp_load_cg_dim_customer]
AS
BEGIN
    -- First, clear out all existing data from the dimension table
    TRUNCATE TABLE [CG_Data_Warehouse].[stg].[cg_dim_customer];

    -- Next, insert the fresh, distinct customer data from the source table
    INSERT INTO [CG_Data_Warehouse].[stg].[cg_dim_customer]
    (
        customer_name, contact_first_name, contact_last_name, phone,
        addressline1, addressline2, city, state, postalcode, country, territory
    )
    SELECT DISTINCT
        CUSTOMERNAME,
        CONTACTFIRSTNAME,
        CONTACTLASTNAME,
        PHONE,
        ADDRESSLINE1,
        ADDRESSLINE2,
        CITY,
        STATE,
        POSTALCODE,
        COUNTRY,
        TERRITORY
    FROM
        [CG_Data_Warehouse].[stg].[sales_data_sample]
    WHERE
        CUSTOMERNAME IS NOT NULL;
END
GO