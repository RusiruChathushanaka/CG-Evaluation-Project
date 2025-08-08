-- Step 1: Create a reusable function to update the 'updated_at' column
-- This function sets the 'updated_at' field to the current time.
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = now(); 
   RETURN NEW;
END;
$$ language 'plpgsql';

-- Step 2: Add timestamp columns and triggers to each table

-- For cg_dim_date
ALTER TABLE cg_dim_date
ADD COLUMN created_at TIMESTAMPTZ DEFAULT now(),
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT now();

CREATE TRIGGER update_cg_dim_date_updated_at
BEFORE UPDATE ON cg_dim_date
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- For cg_dim_product
ALTER TABLE cg_dim_product
ADD COLUMN created_at TIMESTAMPTZ DEFAULT now(),
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT now();

CREATE TRIGGER update_cg_dim_product_updated_at
BEFORE UPDATE ON cg_dim_product
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- For cg_dim_customer
ALTER TABLE cg_dim_customer
ADD COLUMN created_at TIMESTAMPTZ DEFAULT now(),
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT now();

CREATE TRIGGER update_cg_dim_customer_updated_at
BEFORE UPDATE ON cg_dim_customer
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- For cg_dim_order
ALTER TABLE cg_dim_order
ADD COLUMN created_at TIMESTAMPTZ DEFAULT now(),
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT now();

CREATE TRIGGER update_cg_dim_order_updated_at
BEFORE UPDATE ON cg_dim_order
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- For cg_fact_sales
ALTER TABLE cg_fact_sales
ADD COLUMN created_at TIMESTAMPTZ DEFAULT now(),
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT now();

CREATE TRIGGER update_cg_fact_sales_updated_at
BEFORE UPDATE ON cg_fact_sales
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();