-- Create the function to insert data into ORDER_FACT
CREATE OR REPLACE FUNCTION Insert_Into_Order_Fact()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    -- Step 1: Create the ORDER_FACT table if it doesn't exist
    EXECUTE '
        CREATE TABLE IF NOT EXISTS ORDER_FACT (
            order_id INTEGER,                       -- Order ID (fact)
            quantity INT NOT NULL CHECK (quantity > 0),        -- Quantity ordered (fact)
            total_price NUMERIC(10, 2) NOT NULL,               -- Total price for the order (fact)
            product_surrogate_id INT NOT NULL,                 -- Surrogate key referencing product dimension
			product VARCHAR(255) NOT NULL,
            time_id VARCHAR(30) NOT NULL,             -- Foreign key linking to the time dimension
            address_id VARCHAR(30) NOT NULL,                           -- Foreign key linking to the address dimension

            -- Product Dimension columns (not part of the fact, but stored for performance)
            product_start_date DATE,                           -- Start Date from Product Dimension
            product_end_date DATE,                             -- End Date from Product Dimension
            product_current_status BOOLEAN,                    -- Current Status from Product Dimension
            product_parent_id VARCHAR(255),                    -- Parent ID from Product Dimension
            product_category VARCHAR(255),                     -- Category Name from Product Dimension

            -- Time Dimension columns
            time_name DATE,                                    -- Time Name from Time Dimension
            time_parent VARCHAR(255),                          -- Time Parent from Time Dimension
            time_level VARCHAR(255),                           -- Time Level from Time Dimension
            time_branch VARCHAR(255),                          -- Time Branch from Time Dimension

            -- Address Dimension columns
            address_name VARCHAR(255),                         -- Address Name from Location Hierarchy
            address_type VARCHAR(100),                         -- Address Type from Location Hierarchy
            parent_id VARCHAR(255),                            -- Parent ID from Location Hierarchy
            address_level INT,                                 -- Address Level from Location Hierarchy

            -- Foreign Key Constraints
            FOREIGN KEY (product_surrogate_id) REFERENCES Product_Staging_Dimension("ID"),
            FOREIGN KEY (address_id) REFERENCES location_hierarchy("address_id")
        );
    ';

    -- Step 2: Insert data into ORDER_FACT based on cleaned Sales_Landing data
    INSERT INTO ORDER_FACT (
        quantity, 
        total_price, 
        product_surrogate_id, 
		product,
        time_id, 
        address_id,
        order_id, 
        product_start_date,
        product_end_date,
        product_current_status,
        product_parent_id,
        product_category,
        time_name,
        time_parent,
        time_level,
        time_branch,
        address_name,
        address_type,
        parent_id,
        address_level
    )
    SELECT 
        sl2."Quantity Ordered", 
        (sl2."Quantity Ordered" * p."Price Each") AS total_price,  -- Calculate total price
        p."ID" AS product_surrogate_id,                             -- Surrogate key from Product Dimension
		p."Product" AS product,
        t."time_id",                                                -- Time ID from Time Dimension
        a."address_id",                                             -- Address ID from Location Hierarchy

        -- Additional Columns
        sl2."Order ID",                                             -- Order ID from ORDER_FACT
        p."StartDate" AS product_start_date,                         -- Start Date from Product Dimension
        p."EndDate" AS product_end_date,                             -- End Date from Product Dimension
        p."CurrStatus" AS product_current_status,                    -- Product Current Status
        p."ParentID" AS product_parent_id,                           -- Product Parent ID
        p."CategoryName" AS product_category,                        -- Product Category

        t."time_name" AS time_name,                                  -- Time Name from Time Dimension
        t."time_parent" AS time_parent,                              -- Time Parent from Time Dimension
        t."time_lvl" AS time_level,                                  -- Time Level from Time Dimension
        t."timebranch" AS time_branch,                               -- Time Branch from Time Dimension

        a."address_name" AS address_name,                            -- Address Name from Location Hierarchy
        a."address_type" AS address_type,                            -- Address Type from Location Hierarchy
        a."parent_id" AS parent_id,                                  -- Parent ID from Location Hierarchy
        a."level" AS address_level                                   -- Address Level from Location Hierarchy
    FROM 
        sales_landing_2 sl2
    JOIN 
        Product_Staging_Dimension p ON UPPER(sl2."Product") = UPPER(p."Product")  -- Match product
    JOIN 
        (SELECT time_id, time_name::DATE time_name, time_parent, time_lvl, timebranch
         FROM time_dimension WHERE time_lvl='0' AND timebranch='main') t 
        ON sl2."Order Date"::DATE = t."time_name"::DATE  -- Match Order Date with Time Dimension
    JOIN 
        location_hierarchy a ON sl2."Purchase Address" LIKE '%' || a."address_name" || '%'  -- Match address
    WHERE 
        p."CurrStatus" = TRUE  -- Only active products
        AND sl2."Order Date" IS NOT NULL;  -- Ensure valid order date
END;
$$;

/*
SELECT Insert_Into_Order_Fact();

SELECT * FROM Order_Fact */
