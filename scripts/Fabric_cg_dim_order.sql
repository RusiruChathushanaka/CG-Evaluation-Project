-- =================================================================
-- 1. Create the Order Dimension Table
-- =================================================================
CREATE TABLE [CG_Data_Warehouse].[stg].[cg_dim_order]
(
   order_number INT NOT NULL,
   status VARCHAR(20)
);
GO

-- =================================================================
-- 2. Insert Initial Data into the Order Dimension Table
-- =================================================================
INSERT INTO [CG_Data_Warehouse].[stg].[cg_dim_order]
(
    order_number,
    status
)
SELECT DISTINCT
    CAST(ORDERNUMBER AS INT),
    STATUS
FROM
    [CG_Data_Warehouse].[stg].[sales_data_sample]
WHERE
    ORDERNUMBER IS NOT NULL;
GO

-- =================================================================
-- 3. Create the Stored Procedure to Load/Refresh Order Data
-- This automates the process for future use.
-- =================================================================
CREATE PROC [stg].[stp_load_cg_dim_order]
AS
BEGIN
    -- First, clear out all existing data from the dimension table
    TRUNCATE TABLE [CG_Data_Warehouse].[stg].[cg_dim_order];

    -- Next, insert the fresh, distinct order data from the source table
    INSERT INTO [CG_Data_Warehouse].[stg].[cg_dim_order]
    (
        order_number,
        status
    )
    SELECT DISTINCT
        CAST(ORDERNUMBER AS INT),
        STATUS
    FROM
        [CG_Data_Warehouse].[stg].[sales_data_sample]
    WHERE
        ORDERNUMBER IS NOT NULL;
END
GO