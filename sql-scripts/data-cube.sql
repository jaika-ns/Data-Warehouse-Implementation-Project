CREATE OR REPLACE FUNCTION create_sales_data_cube()
RETURNS void
LANGUAGE plpgsql
AS $$ 
BEGIN
    -- Drop the existing table if it exists
    DROP TABLE IF EXISTS sales_data_cube;

    -- Try to create the new data cube table
    BEGIN
        CREATE TABLE sales_data_cube AS
        SELECT 
            product_surrogate_id,
            address_id, 
            time_id,
            SUM(total_price) AS total_price_sum
        FROM ORDER_FACT
        GROUP BY CUBE(product_surrogate_id, time_id, address_id);
        
        -- Raise a notice when table is created successfully
        RAISE NOTICE 'sales_data_cube table created successfully.';
    EXCEPTION WHEN OTHERS THEN
        -- Capture any errors and raise a notice
        RAISE EXCEPTION 'Error creating table: %', SQLERRM;
    END;

    -- Function completes successfully
    RETURN;
END;
$$;

/*
SELECT create_sales_data_cube();

SELECT * FROM sales_data_cube ORDER BY product_surrogate_id;

*/