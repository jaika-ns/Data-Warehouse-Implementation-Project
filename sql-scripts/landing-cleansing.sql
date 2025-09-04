-- Create the Sales_Landing table
CREATE TABLE Sales_Landing (
    "Order ID" VARCHAR(20),
    "Product" VARCHAR(100),
    "Quantity Ordered" VARCHAR(20),
    "Price Each" VARCHAR(20),
    "Order Date" VARCHAR(20),
    "Purchase Address" VARCHAR(200),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create the clean function for Sales_Landing table
CREATE OR REPLACE FUNCTION Clean_Sales_Landing()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    executed_time TIMESTAMP := NOW();  -- Timestamp for the current session
    max_updated_at TIMESTAMP;  -- Variable to store the maximum updated_at value
    sales_cur CURSOR FOR 
        SELECT  
            "Order ID", 
            TRIM("Product") AS "Product", 
            "Quantity Ordered", 
            "Price Each", 
            "Order Date", 
            TRIM("Purchase Address") AS "Purchase Address",
            updated_at
        FROM Sales_Landing sl
        WHERE sl.updated_at >= max_updated_at  -- Only process records updated after the max_updated_at
        AND NOT EXISTS (
            SELECT 1
            FROM sales_landing_2 sl2
            WHERE TRIM(sl2."Order ID"::TEXT) = TRIM(sl."Order ID")
              AND LOWER(TRIM(sl2."Product")) = LOWER(TRIM(sl."Product"))
              AND TRIM(sl2."Quantity Ordered"::TEXT) = TRIM(sl."Quantity Ordered")
              AND TRIM(sl2."Price Each"::TEXT) = TRIM(sl."Price Each")
              AND sl2."Order Date"::TEXT = sl."Order Date"
              AND TRIM(sl2."Purchase Address") = TRIM(sl."Purchase Address")
        );
    record RECORD;
BEGIN
    -- Step 1: Create the sales_landing_2 table if it doesn't exist
    EXECUTE '
        CREATE TABLE IF NOT EXISTS sales_landing_2 (
            "Order ID" INT,
            "Product" VARCHAR(255),
            "Quantity Ordered" INT CHECK ("Quantity Ordered" > 0),
            "Price Each" DECIMAL CHECK ("Price Each" > 0),
            "Order Date" TIMESTAMP,
            "Purchase Address" VARCHAR(255),
            updated_at TIMESTAMP DEFAULT NOW()  -- Track when records are updated
        );
    ';
    
    -- Step 2: Create the error table if it doesn't exist
    EXECUTE '
        CREATE TABLE IF NOT EXISTS sales_landing_errors (
            "Error ID" SERIAL PRIMARY KEY,
            "Order ID" VARCHAR(255),
            "Product" VARCHAR(255),
            "Quantity Ordered" VARCHAR(255),
            "Price Each" VARCHAR(255),
            "Order Date" VARCHAR(255),
            "Purchase Address" VARCHAR(255),
            "Error Reason" TEXT,
            "Logged At" TIMESTAMP DEFAULT NOW(),
            executed_time TIMESTAMP  -- Track when the error occurred
        )
    ';

    -- Step 3: Get the maximum updated_at value from Sales_Landing
    SELECT MAX(updated_at) INTO max_updated_at
    FROM Sales_Landing;

    -- Step 4: Open the cursor to process records
    OPEN sales_cur;

    LOOP
        -- Fetch the next record from the cursor
        FETCH sales_cur INTO record;

        -- Exit the loop when no more records are found
        EXIT WHEN NOT FOUND;

        -- Only process records with a valid "Order ID" (numeric)
        IF record."Order ID" ~ '^\d+$' THEN  -- Check if "Order ID" is numeric
            -- Apply cleaning conditions
            IF record."Product" IS NOT NULL  -- "Product" must not be NULL
               AND record."Quantity Ordered" ~ '^\d+$' -- "Quantity Ordered" must be numeric
               AND record."Quantity Ordered"::INTEGER > 0 -- "Quantity Ordered" must be positive
               AND record."Price Each" ~ '^\d+(\.\d{1,2})?$' -- "Price Each" must be a valid decimal
               AND record."Price Each"::DECIMAL > 0 -- "Price Each" must be positive
               AND record."Order Date" IS NOT NULL -- "Order Date" must not be NULL
               AND TO_TIMESTAMP(record."Order Date", 'MM/DD/YY HH24:MI') <= NOW() -- "Order Date" must not be in the future
               AND record."Purchase Address" IS NOT NULL THEN

                -- Insert valid data into the cleaned table
                INSERT INTO sales_landing_2 (
                    "Order ID", "Product", "Quantity Ordered", "Price Each", "Order Date", "Purchase Address", updated_at
                ) VALUES (
                    record."Order ID"::INTEGER,  -- Safe cast for "Order ID"
                    UPPER(record."Product"),  -- Convert "Product" to uppercase
                    record."Quantity Ordered"::INTEGER,
                    record."Price Each"::DECIMAL,
                    TO_TIMESTAMP(record."Order Date", 'MM/DD/YY HH24:MI'),
                    record."Purchase Address",
                    executed_time  -- Use executed_time for when this session was processed
                );
            ELSE
                -- Log invalid records into the error table
                INSERT INTO sales_landing_errors (
                    "Order ID", "Product", "Quantity Ordered", "Price Each", "Order Date", "Purchase Address", "Error Reason", executed_time
                ) VALUES (
                    record."Order ID",
                    record."Product",
                    record."Quantity Ordered",
                    record."Price Each",
                    record."Order Date",
                    record."Purchase Address",
                    CASE
                        WHEN record."Order ID" !~ '^\d+$' THEN 'Invalid Order ID'
                        WHEN record."Product" IS NULL THEN 'Product is NULL'
                        WHEN record."Quantity Ordered" !~ '^\d+$' THEN 'Invalid Quantity Ordered'
                        WHEN record."Quantity Ordered"::INTEGER <= 0 THEN 'Quantity Ordered is not positive'
                        WHEN record."Price Each" !~ '^\d+(\.\d{1,2})?$' THEN 'Invalid Price Each format'
                        WHEN record."Price Each"::DECIMAL <= 0 THEN 'Price Each is not positive'
                        WHEN record."Order Date" IS NULL THEN 'Order Date is NULL'
                        WHEN TO_TIMESTAMP(record."Order Date", 'MM/DD/YY HH24:MI') > NOW() THEN 'Order Date is in the future'
                        WHEN record."Purchase Address" IS NULL THEN 'Purchase Address is NULL'
                        ELSE 'Unknown validation error'
                    END,
                    executed_time  -- Use executed_time for when the error was logged
                );
            END IF;
        ELSE
            -- Log invalid Order ID for non-numeric entries
            INSERT INTO sales_landing_errors (
                "Order ID", "Product", "Quantity Ordered", "Price Each", "Order Date", "Purchase Address", "Error Reason", executed_time
            ) VALUES (
                record."Order ID",
                record."Product",
                record."Quantity Ordered",
                record."Price Each",
                record."Order Date",
                record."Purchase Address",
                'Invalid Order ID (non-numeric)',
                executed_time  -- Use executed_time for when the error was logged
            );
        END IF;
    END LOOP;

    -- Close the cursor
    CLOSE sales_cur;

    RETURN;
END;
$$;

-- Create trigger function for the Sales_Landing table
CREATE OR REPLACE FUNCTION Trigger_Clean_Sales_Landing()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$ 
BEGIN
    -- Call the main cleaning function
    PERFORM Clean_Sales_Landing();
	
	-- Call time dimension
	PERFORM funct_create_date_dimension();
	PERFORM funct_update_date_dimension();
	
	-- Call address dimension
	PERFORM Clean_Address_Process();
	
	-- Call product dimension
	PERFORM Populate_Product();
	
	PERFORM Insert_Into_Order_Fact();
	
	PERFORM create_sales_data_cube();
	
    RETURN NULL;
END;
$$;

-- Create trigger to invoke cleaning function after insert
CREATE TRIGGER After_Insert_Clean_Sales
AFTER INSERT
ON Sales_Landing
FOR EACH STATEMENT
EXECUTE FUNCTION Trigger_Clean_Sales_Landing();
