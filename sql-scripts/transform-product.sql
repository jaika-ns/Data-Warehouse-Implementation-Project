-- Function to insert product records with a unique ProductID
CREATE OR REPLACE FUNCTION Populate_Product()
RETURNS VOID LANGUAGE plpgsql
AS $$
DECLARE
    next_id INTEGER;
    product_record RECORD;
    prod_id VARCHAR(255);
    cursor_name CURSOR FOR 
        SELECT sl."Product", sl."Price Each"::DECIMAL, MIN(sl."Order Date")::TIMESTAMP AS "StartDate",
            CASE
                WHEN UPPER(sl."Product") SIMILAR TO '%(MONITOR)%' THEN 'CAT-00001'
                WHEN UPPER(sl."Product") SIMILAR TO '%(BATTERIES|BATTERY)%' THEN 'CAT-00002'
                WHEN UPPER(sl."Product") SIMILAR TO '%(HEADPHONES)%' THEN 'CAT-00003'
                WHEN UPPER(sl."Product") SIMILAR TO '%(TV)%' THEN 'CAT-00004'
                WHEN UPPER(sl."Product") SIMILAR TO '%(DRYER|WASHING)%' THEN 'CAT-00005'
                WHEN UPPER(sl."Product") SIMILAR TO '%(PHONE)%' THEN 'CAT-00006'
                WHEN UPPER(sl."Product") SIMILAR TO '%(LAPTOP)%' THEN 'CAT-00007'
                WHEN UPPER(sl."Product") SIMILAR TO '%(CABLE)%' THEN 'CAT-00008'
                ELSE 'CAT-00009'
            END AS "ParentID",
            CASE
                WHEN UPPER(sl."Product") SIMILAR TO '%(MONITOR)%' THEN 'MONITOR'
                WHEN UPPER(sl."Product") SIMILAR TO '%(BATTERIES|BATTERY)%' THEN 'BATTERY'
                WHEN UPPER(sl."Product") SIMILAR TO '%(HEADPHONES)%' THEN 'HEADPHONES'
                WHEN UPPER(sl."Product") SIMILAR TO '%(TV)%' THEN 'TV'
                WHEN UPPER(sl."Product") SIMILAR TO '%(DRYER|WASHING)%' THEN 'APPLIANCES'
                WHEN UPPER(sl."Product") SIMILAR TO '%(PHONE)%' THEN 'PHONE'
                WHEN UPPER(sl."Product") SIMILAR TO '%(LAPTOP)%' THEN 'LAPTOP'
                WHEN UPPER(sl."Product") SIMILAR TO '%(CABLE)%' THEN 'ACCESSORIES'
                ELSE 'OTHERS'
            END AS "CategoryName"
        FROM Sales_Landing_2 sl
        LEFT JOIN Product_Staging_Dimension psd
            ON sl."Product" = psd."Product"
            AND ROUND(sl."Price Each"::DECIMAL, 2) = ROUND(psd."Price Each"::DECIMAL, 2)
        WHERE psd."ProductID" IS NULL
        GROUP BY sl."Product", sl."Price Each";
BEGIN
    -- Ensure Product_Staging_Dimension exists
    EXECUTE 'CREATE TABLE IF NOT EXISTS Product_Staging_Dimension (
        "ID" BIGSERIAL PRIMARY KEY,
        "ProductID" VARCHAR(255),
        "Product" VARCHAR(255),
        "Price Each" DECIMAL,
        "StartDate" TIMESTAMP,
        "EndDate" TIMESTAMP DEFAULT NULL,
        "CurrStatus" BOOLEAN DEFAULT TRUE,
        "ParentID" VARCHAR(255),
        "CategoryName" VARCHAR(255)
    );';

    -- Generate next ProductID sequence (Based on product name)
    SELECT COALESCE(MAX(CAST(SUBSTRING("ProductID", 6) AS INTEGER)), 0) 
    INTO next_id
    FROM Product_Staging_Dimension;

    -- Open the cursor for the new products
    OPEN cursor_name;
	
	-- Loop through each record in the cursor
	LOOP
		FETCH cursor_name INTO product_record;
		EXIT WHEN NOT FOUND; -- Exit the loop when no more records are found

		-- Check if the product exists with the same name
		PERFORM 1
		FROM Product_Staging_Dimension psd
		WHERE psd."Product" = product_record."Product";

		IF NOT FOUND THEN
			-- If the product doesn't exist, insert a new record with a new ProductID
			prod_id := 'PROD-' || LPAD((next_id + 1)::TEXT, 5, '0');
			INSERT INTO Product_Staging_Dimension (
				"ProductID", "Product", "Price Each", "StartDate", "ParentID", "CategoryName"
			)
			VALUES (prod_id, product_record."Product", product_record."Price Each", product_record."StartDate", product_record."ParentID", product_record."CategoryName");
			next_id := next_id + 1; -- Increment next_id for the next product
		ELSE
			-- If the product exists, check if the price has changed
			PERFORM 1
			FROM Product_Staging_Dimension psd
			WHERE psd."Product" = product_record."Product"
			AND ROUND(psd."Price Each"::DECIMAL, 2) != ROUND(product_record."Price Each"::DECIMAL, 2)
			AND psd."CurrStatus" = TRUE;  -- Only check for the active version

			IF FOUND THEN
				-- If price has changed, update the current version and add a new version with the same ProductID
				UPDATE Product_Staging_Dimension
				SET "EndDate" = product_record."StartDate" - INTERVAL '1 day',
					"CurrStatus" = FALSE
				WHERE "Product" = product_record."Product"
				  AND "CurrStatus" = TRUE;

				-- Insert the new version with the same ProductID
				INSERT INTO Product_Staging_Dimension (
					"ProductID", "Product", "Price Each", "StartDate", "ParentID", "CategoryName", "CurrStatus"
				)
				VALUES (
					(SELECT "ProductID" FROM Product_Staging_Dimension WHERE "Product" = product_record."Product" LIMIT 1),
					product_record."Product", 
					product_record."Price Each", 
					product_record."StartDate", 
					product_record."ParentID", 
					product_record."CategoryName", 
					TRUE
				);
			END IF;
		END IF;
	END LOOP;

    -- Close the cursor
    CLOSE cursor_name;

    -- Call UpdateProductVersioning to handle products with price changes
    CALL UpdateProductVersioning();

    EXECUTE 'DROP TABLE IF EXISTS Orders_Updated;';

    -- Assign ProductID to order records using surrogate key
    EXECUTE 'CREATE TEMP TABLE Orders_Updated AS
    SELECT
        sl."Order ID",
        sl."Product",
        d."ID" AS "ProductID", -- Use surrogate key
        d."ParentID" AS "CategoryID",
        sl."Price Each",
        sl."Order Date"
    FROM Sales_Landing_2 sl
    JOIN Product_Staging_Dimension d
        ON sl."Product" = d."Product"
       AND ROUND(sl."Price Each"::DECIMAL, 2) = ROUND(d."Price Each"::DECIMAL, 2)
       AND sl."Order Date" BETWEEN d."StartDate" AND COALESCE(d."EndDate", ''9999-12-31'');';

    RETURN;
END;
$$;



CREATE OR REPLACE PROCEDURE UpdateProductVersioning()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Step 1: Mark previous versions as false for records with earlier dates
    UPDATE Product_Staging_Dimension
    SET 
       "EndDate" = COALESCE(
            (SELECT MIN(sl."Order Date") - INTERVAL '1 day'
             FROM Sales_Landing_2 sl
             WHERE sl."Product" = Product_Staging_Dimension."Product"
               AND ROUND(sl."Price Each"::DECIMAL, 2) != ROUND(Product_Staging_Dimension."Price Each"::DECIMAL, 2)
               AND sl."Order Date" > Product_Staging_Dimension."StartDate"), 
            '9999-12-31'),
        "CurrStatus" = FALSE
    WHERE "CurrStatus" = TRUE
      AND EXISTS (
          SELECT 1
          FROM Sales_Landing_2 sl
          WHERE sl."Product" = Product_Staging_Dimension."Product"
            AND ROUND(sl."Price Each"::DECIMAL, 2) != ROUND(Product_Staging_Dimension."Price Each"::DECIMAL, 2)
            AND sl."Order Date" > Product_Staging_Dimension."StartDate"
      );

    -- Step 2: Mark latest record as active
    UPDATE Product_Staging_Dimension
    SET "CurrStatus" = TRUE,
        "EndDate" = NULL
    WHERE "Product" = Product_Staging_Dimension."Product"
      AND "StartDate" = (
          SELECT MAX("StartDate")
          FROM Product_Staging_Dimension psd3
          WHERE psd3."Product" = Product_Staging_Dimension."Product"
      );

    RAISE NOTICE 'Product versioning updates completed.';
END;
$$;

/*
DO $$ 
BEGIN
    PERFORM Populate_Product();
END $$;

-- Test Outputs
SELECT * FROM Sales_Landing_2;
SELECT * FROM Product_Staging_Dimension ORDER BY "ProductID", "Product", "StartDate";
SELECT * FROM Orders_Updated;

SELECT "ProductID", "Product", "Price Each", "StartDate", "EndDate", "CurrStatus", "ParentID", "CategoryName"
FROM Product_Staging_Dimension ORDER BY "ProductID", "Product", "StartDate"; */