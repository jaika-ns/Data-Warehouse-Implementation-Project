CREATE OR REPLACE FUNCTION Populate_Address() 
RETURNS TABLE ("Purchase ID" INT , "Street" VARCHAR(100), "City" VARCHAR(50), "State" VARCHAR(20), "Postal Code" VARCHAR(20)) 
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS Purchase_Address_Staging;
    CREATE TABLE Purchase_Address_Staging(
        "Purchase ID" SERIAL PRIMARY KEY, 	
        "Street" VARCHAR(100),
        "City" VARCHAR(50),
        "State" VARCHAR(20),
        "Postal Code" VARCHAR(20)
    );

    INSERT INTO Purchase_Address_Staging("Street", "City", "State", "Postal Code")
    SELECT 
        TRIM(split_part("Purchase Address", ',', 1)) AS "Street", 
        TRIM(split_part("Purchase Address", ',', 2)) AS "City",
        TRIM(split_part(split_part("Purchase Address", ',', 3), ' ', 2)) AS "State",
        TRIM(split_part("Purchase Address", ' ', -1)) AS "Postal Code"
    FROM Sales_Landing;

    RETURN QUERY SELECT * FROM Purchase_Address_Staging;
END;
$$;

---------

CREATE OR REPLACE FUNCTION Clean_Duplicates_Address()
RETURNS TABLE ("Purchase ID" INT, "Street" VARCHAR(50), "City" VARCHAR(50), "State" VARCHAR(20), "Postal Code" VARCHAR(20)) 
LANGUAGE plpgsql
AS $$
BEGIN

    DROP TABLE IF EXISTS Purchase_Address_Staging_2;
    CREATE TABLE Purchase_Address_Staging_2(
        "Purchase ID" SERIAL PRIMARY KEY, 
        "Street" VARCHAR(50),
        "City" VARCHAR(50),
        "State" VARCHAR(20),
        "Postal Code" VARCHAR(20)
    );

    INSERT INTO Purchase_Address_Staging_2("Street", "City", "State", "Postal Code")
    SELECT DISTINCT 
        pas."Street", 
        pas."City", 
        pas."State", 
        pas."Postal Code"
    FROM Purchase_Address_Staging AS pas
    WHERE pas."Street" IS NOT NULL 
        AND pas."City" IS NOT NULL
        AND pas."State" IS NOT NULL
        AND pas."Postal Code" IS NOT NULL;

    RETURN QUERY SELECT * FROM Purchase_Address_Staging_2;
END;
$$;

----------

CREATE OR REPLACE FUNCTION Clean_Invalid_Postal_Codes()
RETURNS TABLE ("Purchase ID" INTEGER, "Street" VARCHAR(50), "City" VARCHAR(50), "State" VARCHAR(20), "Postal Code" VARCHAR(20)) 
LANGUAGE plpgsql
AS $$
BEGIN

    DROP TABLE IF EXISTS Purchase_Address_Staging_3;

    CREATE TABLE Purchase_Address_Staging_3(
        "Purchase ID" SERIAL PRIMARY KEY,
        "Street" VARCHAR(50),
        "City" VARCHAR(50),
        "State" VARCHAR(20),
        "Postal Code" VARCHAR(20)
    );

    INSERT INTO Purchase_Address_Staging_3("Street", "City", "State", "Postal Code")
    SELECT 
        pas2."Street", 
        pas2."City", 
        pas2."State", 
        pas2."Postal Code"
    FROM Purchase_Address_Staging_2 AS pas2
    WHERE pas2."Postal Code" ~ '^\d+$';

    RETURN QUERY SELECT * FROM Purchase_Address_Staging_3;
END;
$$;

---------

CREATE OR REPLACE FUNCTION Clean_Purchase_Address()
RETURNS TABLE ("Purchase ID" INTEGER, "Street" VARCHAR(50), "City" VARCHAR(50), "State" VARCHAR(20), "Postal Code" VARCHAR(20)) 
LANGUAGE plpgsql
AS $$
BEGIN

    DROP TABLE IF EXISTS Purchase_Address_Final_Staging;

    CREATE TABLE Purchase_Address_Final_Staging(
        "Purchase ID" SERIAL PRIMARY KEY,
        "Street" VARCHAR(50) NOT NULL,
        "City" VARCHAR(50) NOT NULL,
        "State" VARCHAR(20) NOT NULL,
        "Postal Code" VARCHAR(20) NOT NULL
    );

    INSERT INTO Purchase_Address_Final_Staging("Street", "City", "State", "Postal Code")
    SELECT 
        TRIM(pas3."Street") AS "Street", 
        TRIM(pas3."City") AS "City", 
        UPPER(pas3."State") AS "State", 
        pas3."Postal Code" AS "Postal Code"
    FROM Purchase_Address_Staging_3 AS pas3;

    RETURN QUERY SELECT * FROM Purchase_Address_Final_Staging;
END;
$$;

------

CREATE OR REPLACE FUNCTION Populate_Location_Hierarchy()
RETURNS TABLE (address_id VARCHAR, address_name VARCHAR(100), address_type VARCHAR(50), parent_id VARCHAR, level INT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_state_id VARCHAR;
    v_postal_code_id VARCHAR;
    v_city_id VARCHAR;
    v_street_id VARCHAR;
    v_parent_address_id VARCHAR;
    record RECORD;
    v_count INTEGER;
BEGIN
    DROP TABLE IF EXISTS Location_Hierarchy;

    CREATE TABLE Location_Hierarchy(
        address_id VARCHAR PRIMARY KEY,
        address_name VARCHAR(100),
        address_type VARCHAR(50),
        parent_id VARCHAR REFERENCES Location_Hierarchy(address_id),
        level INT
    );

    INSERT INTO Location_Hierarchy (address_id, address_name, address_type, level)
    SELECT DISTINCT 
        'S-' || "State", 
        "State", 
        'State', 
        3
    FROM Purchase_Address_Final_Staging;

    FOR record IN (SELECT DISTINCT "Postal Code", "State" FROM Purchase_Address_Final_Staging) LOOP
        SELECT lh.address_id INTO v_parent_address_id
        FROM Location_Hierarchy lh
        WHERE lh.address_name = record."State" AND lh.level = 3;

        v_postal_code_id := 'S-' || record."State" || TRIM(record."Postal Code");

        INSERT INTO Location_Hierarchy (address_id, address_name, address_type, parent_id, level)
        VALUES (v_postal_code_id, record."Postal Code", 'Postal Code', v_parent_address_id, 2);
    END LOOP;

	FOR record IN (SELECT DISTINCT "City", "Postal Code" FROM Purchase_Address_Final_Staging) LOOP
		SELECT lh.address_id INTO v_parent_address_id
		FROM Location_Hierarchy lh
		WHERE lh.address_name = record."Postal Code" AND lh.level = 2;

		SELECT MAX(CAST(SUBSTRING(lh.address_id, LENGTH(lh.address_id) - 4, 5) AS INTEGER)) INTO v_count
		FROM Location_Hierarchy lh
		WHERE lh.parent_id = v_parent_address_id AND lh.level = 1;

		IF v_count IS NULL THEN
			v_count := 0;
		END IF;

		v_city_id := v_parent_address_id || '-C' || LPAD((v_count + 1)::TEXT, 5, '0');

		INSERT INTO Location_Hierarchy (address_id, address_name, address_type, parent_id, level)
		VALUES (v_city_id, record."City", 'City', v_parent_address_id, 1);
	END LOOP;

	FOR record IN (SELECT DISTINCT "Street", "City", "Postal Code" FROM Purchase_Address_Final_Staging) LOOP
		SELECT lh.address_id INTO v_parent_address_id
		FROM Location_Hierarchy lh
		WHERE lh.address_name = record."City" AND lh.level = 1;

		SELECT MAX(CAST(SUBSTRING(lh.address_id, LENGTH(lh.address_id) - 4, 5) AS INTEGER)) INTO v_count
		FROM Location_Hierarchy lh
		WHERE lh.parent_id = v_parent_address_id AND lh.level = 0;

		IF v_count IS NULL THEN
			v_count := 0;
		END IF;

		v_street_id := v_parent_address_id || '-S' || LPAD((v_count + 1)::TEXT, 5, '0');

		INSERT INTO Location_Hierarchy (address_id, address_name, address_type, parent_id, level)
		VALUES (v_street_id, record."Street", 'Street', v_parent_address_id, 0);
	END LOOP;

    RETURN QUERY SELECT * FROM Location_Hierarchy ORDER BY level, address_id;
END;
$$; 

------

CREATE OR REPLACE FUNCTION Clean_Address_Process()
RETURNS VOID
LANGUAGE plpgsql
AS $$ 
BEGIN
    PERFORM Populate_Address();
    PERFORM Clean_Duplicates_Address();
    PERFORM Clean_Invalid_Postal_Codes();
    PERFORM Clean_Purchase_Address();
    PERFORM Populate_Location_Hierarchy();
    
END;
$$;


