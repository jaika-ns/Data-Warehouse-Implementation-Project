
CREATE OR REPLACE FUNCTION funct_create_date_dimension()
RETURNS VOID
LANGUAGE plpgsql
AS $BODY$
	DECLARE
	start_year INT;
	end_year INT;
	day_id INT;
	current_year_iterator INT; --FOR CALCULATION OF IDs AND ITERATIONS

	--DATA MAPPING
	total_months INT;
	starting_date VARCHAR;
	BEGIN 
	day_id := 1;

	IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'time_dimension') THEN
    RAISE NOTICE 'Aborted Procedure';
	--RETURN;
	END IF;

	SELECT MIN(EXTRACT(YEAR FROM "Order Date"))
	INTO
	start_year
	FROM (SELECT * FROM sales_landing_2);

	SELECT MAX(EXTRACT(YEAR FROM "Order Date"))
	INTO
	end_year
	FROM (SELECT * FROM sales_landing_2); 

	DROP TABLE IF EXISTS weekbranch_l0_dummy;
	CREATE TABLE weekbranch_l0_dummy (
	l0_id VARCHAR(30),
	l0_value DATE,
	l0_parent VARCHAR(30),
	l0_lvl INT
	); --DAY TABLE CREATION

	DROP TABLE IF EXISTS l0_dummy;
	CREATE TABLE l0_dummy (
	l0_id VARCHAR(30),
	l0_value DATE,
	l0_parent VARCHAR(30),
	l0_lvl INT
	); --DAY TABLE CREATION

	DROP TABLE IF EXISTS l1_dummy;
	CREATE TABLE l1_dummy (
	l1_id VARCHAR(30),
	l1_value_month INT,
	l1_value_year INT,
	l1_parent VARCHAR(30),
	l1_lvl INT
	); --MONTH TABLE CREATION

	DROP TABLE IF EXISTS l2_dummy;
	CREATE TABLE l2_dummy (
	l2_id VARCHAR(30),
	l2_value INT,
	l2_parent VARCHAR(30),
	l2_lvl INT
	); --YEAR TABLE CREATION

	DROP TABLE IF EXISTS quarter_dummy;
	CREATE TABLE quarter_dummy (
	q_id VARCHAR(30),
	q_value INT,
	q_year INT,
	q_parent VARCHAR(30),
	q_lvl INT
	); --QUARTER TABLE CREATION

	DROP TABLE IF EXISTS h_dummy;
	CREATE TABLE h_dummy (
	h_id VARCHAR(30),
	h_value INT,
	h_year INT,
	h_parent VARCHAR(30),
	h_lvl INT
	); --HALFYEAR TABLE CREATION

	DROP TABLE IF EXISTS w_dummy;
	CREATE TABLE w_dummy (
	w_id VARCHAR(30),
	w_value INT,
	w_day DATE,
	w_month INT,
	w_year INT,
	w_parent VARCHAR(30),
	w_lvl INT
	);

	FOR yr_cnt IN start_year..end_year LOOP
		INSERT INTO l2_dummy(l2_id, l2_value, l2_lvl)
		VALUES ('Y_'||yr_cnt, yr_cnt, 2);
	END LOOP; --POPULATING YEAR TABLE

	FOR yr_cnt IN start_year..end_year LOOP
		FOR mnth_cnt IN 1..12 LOOP
			INSERT INTO l1_dummy(l1_id, l1_value_year, l1_value_month, l1_lvl)
			VALUES (
			'M_'||yr_cnt||mnth_cnt, 
			yr_cnt, mnth_cnt
			,1); --POPULATING MONTH TABLE 
		END LOOP;
	END LOOP;
	
	INSERT INTO l0_dummy(l0_id,l0_value,l0_lvl)
	VALUES(
	'D_'||EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE)||
	'0'||EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE)||
	'0'||EXTRACT('Day' FROM (start_year::VARCHAR||'-01-01')::DATE) 
	--PRIME DAY FOR GENERATION
		
	,(start_year::VARCHAR||'-01-01')::DATE,0);

	SELECT EXTRACT('Year' FROM l0_value) INTO current_year_iterator FROM l0_dummy ORDER BY l0_value DESC LIMIT 1;

	RAISE NOTICE '%', current_year_iterator;
	RAISE NOTICE '%', starting_date;

	INSERT INTO w_dummy(w_id,w_value,w_day,w_month,w_year) 
		VALUES ('W_'||(EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE))::VARCHAR||
		(EXTRACT('Week' FROM (start_year::VARCHAR||'-01-01')::DATE))::VARCHAR,
		EXTRACT('Week' FROM (start_year::VARCHAR||'-01-01')::DATE),
		(start_year::VARCHAR||'-01-01')::DATE,
		EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE),
		EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE)
		); --PRIME WEEK FOR GENERATION

	WHILE current_year_iterator!=end_year+1 LOOP 
		INSERT INTO l0_dummy(l0_id,l0_value,l0_lvl)
		VALUES(
		CASE
			WHEN EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id)<10 AND 
			EXTRACT('Day' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id)<10
		THEN
		'D_'||(EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||'0'||
		(EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||'0'||
		(EXTRACT('Day' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR
		WHEN EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id)<10 
			THEN
			'D_'||(EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||'0'||
			(EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||
			(EXTRACT('Day' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR
		WHEN EXTRACT('Day' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id)<10 
			THEN
			'D_'||(EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||
			(EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||'0'||
			(EXTRACT('Day' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR	
			ELSE 
		'D_'||(EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||
		(EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||
		(EXTRACT('Day' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR
		END
		,
		(start_year::VARCHAR||'-01-01')::DATE+day_id,
		0);

		INSERT INTO w_dummy(w_id,w_value,w_day,w_month,w_year) 
		VALUES ('W_'||(EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||
		(EXTRACT('Week' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR,
		EXTRACT('Week' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id),
		(start_year::VARCHAR||'-01-01')::DATE+day_id,
		EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id),
		EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id)
		);
		
		SELECT EXTRACT('Year' FROM l0_value) INTO current_year_iterator FROM l0_dummy ORDER BY l0_value DESC LIMIT 1;
		day_id := day_id+1;
	END LOOP; --POPULATING DAY TABLE

	DELETE FROM l0_dummy WHERE EXTRACT('Year' FROM l0_value)=current_year_iterator; 
	DELETE FROM w_dummy WHERE EXTRACT(YEAR FROM w_day)=current_year_iterator;	
	--TRIMMING THE EXCESS KEEPING DUPLICATE VALUES ON WEEK 

	UPDATE w_dummy 
	SET w_year = w_year+1, 
		w_id = 'W_'||w_year+1||'1'
	WHERE w_month = 12 AND  w_value=1;

	UPDATE w_dummy 
	SET w_year = w_year-1, 
		w_id = 'W_'||w_year-1||'53'
	WHERE w_month = 1 AND w_value=53;

	UPDATE w_dummy 
	SET w_year = w_year-1, 
		w_id = 'W_'||w_year-1||'52'
	WHERE w_month = 1 AND w_value=52;

	--Reconnecting mismatched weeks. 

	INSERT INTO weekbranch_l0_dummy(l0_id,l0_value,l0_parent,l0_lvl)
	SELECT * FROM l0_dummy; --COPYING VALUES TO NEW TABLE FOR ALTERNATE BRANCH

	FOR yr_cnt IN start_year..end_year LOOP
		FOR q_cnt IN 1..4 LOOP
			INSERT INTO quarter_dummy(q_id, q_value, q_year, q_lvl)
			VALUES (
			'Q_'||yr_cnt||q_cnt, 
			q_cnt, yr_cnt,99); --POPULATING QUARTERS 
		END LOOP;
	END LOOP;

	FOR yr_cnt IN start_year..end_year LOOP
		FOR h_cnt IN 1..2 LOOP
			INSERT INTO h_dummy(h_id, h_value, h_year, h_lvl)
			VALUES (
			'HY_'||yr_cnt||h_cnt, 
			h_cnt,yr_cnt,99); --POPULATING QUARTERS 
		END LOOP;
	END LOOP;

	-- TABLE CREATION END. DATA MAPPING BEGIN

	FOR yr_cnt IN start_year..end_year LOOP 
		UPDATE l1_dummy 
		SET l1_parent=(SELECT l2_id FROM l2_dummy WHERE l2_value=yr_cnt)
		WHERE l1_value_year::INT=((SELECT l2_value FROM l2_dummy WHERE l2_value=yr_cnt));
	END LOOP; --DEFUNCT MONTH YEAR PARENT ASSIGNMENT

	FOR yr_cnt IN start_year..end_year LOOP
		FOR mnth_cnt IN 1..12 LOOP
			UPDATE l0_dummy
			SET l0_parent=(SELECT l1_id FROM l1_dummy WHERE l1_value_month=mnth_cnt AND l1_value_year=yr_cnt)
			WHERE EXTRACT(YEAR FROM l0_value)=yr_cnt AND EXTRACT(MONTH FROM l0_value)=mnth_cnt;
		END LOOP;
	END LOOP; --ASSIGN DAY TO MONTH

	FOR yr_cnt IN start_year..end_year+1 LOOP
		FOR w_cnt IN 1..55 LOOP
			UPDATE weekbranch_l0_dummy
			SET l0_parent=(SELECT DISTINCT w_id FROM w_dummy WHERE w_value=w_cnt AND w_year=yr_cnt)
			WHERE l0_value IN (SELECT DISTINCT w_day FROM w_dummy WHERE w_value=w_cnt AND w_year=yr_cnt);
		END LOOP;
	END LOOP; --ASSIGN DAY TO WEEK

	FOR yr_cnt IN start_year..end_year LOOP
		FOR q_cnt IN 1..4 LOOP
			CASE WHEN q_cnt=1
				THEN
				UPDATE l1_dummy
				SET l1_parent=(SELECT q_id FROM quarter_dummy WHERE q_year=yr_cnt AND q_value=q_cnt)
				WHERE l1_value_year=yr_cnt AND l1_value_month<=3;
				WHEN q_cnt=2
				THEN
				UPDATE l1_dummy
				SET l1_parent=(SELECT q_id FROM quarter_dummy WHERE q_year=yr_cnt AND q_value=q_cnt)
				WHERE l1_value_year=yr_cnt AND l1_value_month<=6 AND l1_value_month>3;
				WHEN q_cnt=3
				THEN
				UPDATE l1_dummy
				SET l1_parent=(SELECT q_id FROM quarter_dummy WHERE q_year=yr_cnt AND q_value=q_cnt)
				WHERE l1_value_year=yr_cnt AND l1_value_month<=9 AND l1_value_month>6;
				WHEN q_cnt=4
				THEN
				UPDATE l1_dummy
				SET l1_parent=(SELECT q_id FROM quarter_dummy WHERE q_year=yr_cnt AND q_value=q_cnt)
				WHERE l1_value_year=yr_cnt AND l1_value_month<=12 AND l1_value_month>9;
			END CASE;
		END LOOP;
	END LOOP; --MONTHS TO HALFYEAR


	FOR yr_cnt IN start_year..end_year LOOP
		FOR q_cnt IN 1..4 LOOP
			CASE WHEN q_cnt=1 OR q_cnt=2 THEN
				UPDATE quarter_dummy
				SET q_parent=(SELECT h_id FROM h_dummy WHERE h_year=yr_cnt AND h_value=1)
				WHERE q_year=yr_cnt AND q_value=q_cnt;
			WHEN q_cnt=3 OR q_cnt=4 THEN
				UPDATE quarter_dummy
				SET q_parent=(SELECT h_id FROM h_dummy WHERE h_year=yr_cnt AND h_value=2)
				WHERE q_year=yr_cnt AND q_value=q_cnt;
			END CASE;
		END LOOP;
	END LOOP; --MAPPING QUARTERS TO HALFYEAR PARENT

	FOR yr_cnt IN start_year..end_year LOOP 
		UPDATE h_dummy 
		SET h_parent=(SELECT l2_id FROM l2_dummy WHERE l2_value=yr_cnt)
		WHERE h_year=((SELECT l2_value FROM l2_dummy WHERE l2_value=yr_cnt));
	END LOOP; --MAPPING HALFYEAR TO YEAR PARENT

	--MAPPING OVER. UNION EVERYTHING
	
	DROP TABLE IF EXISTS time_dimension;
	CREATE TABLE time_dimension(
	time_combined_id VARCHAR(30) PRIMARY KEY,
	time_id VARCHAR(30),
	time_name VARCHAR(30),
	time_parent VARCHAR(30),
	time_lvl VARCHAR(30),
	timebranch VARCHAR(30)
	);

	INSERT INTO time_dimension(time_combined_id, time_id, time_name, time_parent, time_lvl, timebranch)
	SELECT 'M_'||l0_id,
	l0_id, 
	l0_value, 
	l0_parent,
	l0_lvl,
	'main'
	FROM l0_dummy; --INSERTING DAYS

	INSERT INTO time_dimension(time_combined_id, time_id, time_name, time_parent, time_lvl, timebranch)
	SELECT 'W_'||l0_id,
	l0_id, 
	l0_value, 
	l0_parent,
	l0_lvl,
	'week'
	FROM weekbranch_l0_dummy; --INSERTING DAYS FOR WEEK BRANCH

	INSERT INTO time_dimension(time_combined_id,time_id, time_name, time_parent, time_lvl, timebranch)
	SELECT 'M_'||l1_id,
	l1_id, 
	l1_value_year ||'-'|| l1_value_month, 
	l1_parent,
	l1_lvl,
	'main'
	FROM l1_dummy;

	INSERT INTO time_dimension(time_combined_id, time_id, time_name, time_parent, time_lvl, timebranch)
	SELECT 'M_'||l2_id,
	l2_id, 
	l2_value, 
	l2_parent,
	'4',
	'main'
	FROM l2_dummy;

	INSERT INTO time_dimension(time_combined_id, time_id, time_name, time_parent, time_lvl, timebranch)
	SELECT 'M_'||q_id,
	q_id, 
	'Q'||q_value||'-'|| q_year, 
	q_parent,
	'2',
	'main'
	FROM quarter_dummy;

	INSERT INTO time_dimension(time_combined_id, time_id, time_name, time_parent, time_lvl, timebranch)
	SELECT 
	'M_'||h_id,
	h_id, 
	'HY_'||h_value||'-'|| h_year, 
	h_parent,
	'3',
	'main'
	FROM h_dummy;

	INSERT INTO time_dimension(time_combined_id, time_id, time_name, time_parent, time_lvl, timebranch)
	SELECT DISTINCT
	'W_'||w_id,
	w_id, 
	'W_'||w_value||'-'|| w_year, 
	w_parent,
	'1',
	'week'
	FROM w_dummy;

	DROP TABLE IF EXISTS l0_dummy;
	DROP TABLE IF EXISTS weekbranch_l0_dummy;
	DROP TABLE IF EXISTS l1_dummy;
	DROP TABLE IF EXISTS l2_dummy;
	DROP TABLE IF EXISTS quarter_dummy;
	DROP TABLE IF EXISTS h_dummy;
	DROP TABLE IF EXISTS w_dummy;

	RAISE NOTICE 'Finished Generation';

	RETURN;
	END;
$BODY$;

CREATE OR REPLACE FUNCTION funct_update_date_dimension()
RETURNS VOID
LANGUAGE plpgsql
AS $BODY$
	DECLARE
	start_year INT;
	end_year INT;
	day_id INT;
	current_year_iterator INT; --FOR CALCULATION OF IDs AND ITERATIONS

	--DATA MAPPING
	total_months INT;
	starting_date VARCHAR;
	BEGIN 
	day_id := 1;

	IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'time_dimension') THEN
    RAISE NOTICE 'Aborted Procedure';
	RETURN;
	END IF;

	SELECT MIN(EXTRACT(YEAR FROM "Order Date"))
	INTO
	start_year
	FROM (SELECT * FROM sales_landing_2);

	SELECT MAX(EXTRACT(YEAR FROM "Order Date"))
	INTO
	end_year
	FROM (SELECT * FROM sales_landing_2); 


	DROP TABLE IF EXISTS weekbranch_l0_dummy;
	CREATE TABLE weekbranch_l0_dummy (
	l0_id VARCHAR(30),
	l0_value DATE,
	l0_parent VARCHAR(30),
	l0_lvl INT
	); --DAY TABLE CREATION

	DROP TABLE IF EXISTS l0_dummy;
	CREATE TABLE l0_dummy (
	l0_id VARCHAR(30),
	l0_value DATE,
	l0_parent VARCHAR(30),
	l0_lvl INT
	); --DAY TABLE CREATION

	DROP TABLE IF EXISTS l1_dummy;
	CREATE TABLE l1_dummy (
	l1_id VARCHAR(30),
	l1_value_month INT,
	l1_value_year INT,
	l1_parent VARCHAR(30),
	l1_lvl INT
	); --MONTH TABLE CREATION

	DROP TABLE IF EXISTS l2_dummy;
	CREATE TABLE l2_dummy (
	l2_id VARCHAR(30),
	l2_value INT,
	l2_parent VARCHAR(30),
	l2_lvl INT
	); --YEAR TABLE CREATION

	DROP TABLE IF EXISTS quarter_dummy;
	CREATE TABLE quarter_dummy (
	q_id VARCHAR(30),
	q_value INT,
	q_year INT,
	q_parent VARCHAR(30),
	q_lvl INT
	); --QUARTER TABLE CREATION

	DROP TABLE IF EXISTS h_dummy;
	CREATE TABLE h_dummy (
	h_id VARCHAR(30),
	h_value INT,
	h_year INT,
	h_parent VARCHAR(30),
	h_lvl INT
	); --HALFYEAR TABLE CREATION

	DROP TABLE IF EXISTS w_dummy;
	CREATE TABLE w_dummy (
	w_id VARCHAR(30),
	w_value INT,
	w_day DATE,
	w_month INT,
	w_year INT,
	w_parent VARCHAR(30),
	w_lvl INT
	);

	FOR yr_cnt IN start_year..end_year LOOP
		INSERT INTO l2_dummy(l2_id, l2_value, l2_lvl)
		VALUES ('Y_'||yr_cnt, yr_cnt, 2);
	END LOOP; --POPULATING YEAR TABLE

	FOR yr_cnt IN start_year..end_year LOOP
		FOR mnth_cnt IN 1..12 LOOP
			INSERT INTO l1_dummy(l1_id, l1_value_year, l1_value_month, l1_lvl)
			VALUES (
			'M_'||yr_cnt||mnth_cnt, 
			yr_cnt, mnth_cnt
			,1); --POPULATING MONTH TABLE 
		END LOOP;
	END LOOP;
	
	INSERT INTO l0_dummy(l0_id,l0_value,l0_lvl)
	VALUES(
	'D_'||EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE)||
	'0'||EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE)||
	'0'||EXTRACT('Day' FROM (start_year::VARCHAR||'-01-01')::DATE) 
	--PRIME DAY FOR GENERATION
		
	,(start_year::VARCHAR||'-01-01')::DATE,0);

	SELECT EXTRACT('Year' FROM l0_value) INTO current_year_iterator FROM l0_dummy ORDER BY l0_value DESC LIMIT 1;

	RAISE NOTICE '%', current_year_iterator;
	RAISE NOTICE '%', starting_date;

	INSERT INTO w_dummy(w_id,w_value,w_day,w_month,w_year) 
		VALUES ('W_'||(EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE))::VARCHAR||
		(EXTRACT('Week' FROM (start_year::VARCHAR||'-01-01')::DATE))::VARCHAR,
		EXTRACT('Week' FROM (start_year::VARCHAR||'-01-01')::DATE),
		(start_year::VARCHAR||'-01-01')::DATE,
		EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE),
		EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE)
		); --PRIME WEEK FOR GENERATION

	WHILE current_year_iterator!=end_year+1 LOOP 
		INSERT INTO l0_dummy(l0_id,l0_value,l0_lvl)
		VALUES(
		CASE
			WHEN EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id)<10 AND 
			EXTRACT('Day' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id)<10
		THEN
		'D_'||(EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||'0'||
		(EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||'0'||
		(EXTRACT('Day' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR
		WHEN EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id)<10 
			THEN
			'D_'||(EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||'0'||
			(EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||
			(EXTRACT('Day' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR
		WHEN EXTRACT('Day' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id)<10 
			THEN
			'D_'||(EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||
			(EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||'0'||
			(EXTRACT('Day' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR	
			ELSE 
		'D_'||(EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||
		(EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||
		(EXTRACT('Day' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR
		END
		,
		(start_year::VARCHAR||'-01-01')::DATE+day_id,
		0);

		INSERT INTO w_dummy(w_id,w_value,w_day,w_month,w_year) 
		VALUES ('W_'||(EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR||
		(EXTRACT('Week' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id))::VARCHAR,
		EXTRACT('Week' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id),
		(start_year::VARCHAR||'-01-01')::DATE+day_id,
		EXTRACT('Month' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id),
		EXTRACT('Year' FROM (start_year::VARCHAR||'-01-01')::DATE+day_id)
		);
		
		SELECT EXTRACT('Year' FROM l0_value) INTO current_year_iterator FROM l0_dummy ORDER BY l0_value DESC LIMIT 1;
		day_id := day_id+1;
	END LOOP; --POPULATING DAY TABLE

	DELETE FROM l0_dummy WHERE EXTRACT('Year' FROM l0_value)=current_year_iterator; 
	DELETE FROM w_dummy WHERE EXTRACT(YEAR FROM w_day)=current_year_iterator;	
	--TRIMMING THE EXCESS KEEPING DUPLICATE VALUES ON WEEK 

	UPDATE w_dummy 
	SET w_year = w_year+1, 
		w_id = 'W_'||w_year+1||'1'
	WHERE w_month = 12 AND  w_value=1;

	UPDATE w_dummy 
	SET w_year = w_year-1, 
		w_id = 'W_'||w_year-1||'53'
	WHERE w_month = 1 AND w_value=53;

	UPDATE w_dummy 
	SET w_year = w_year-1, 
		w_id = 'W_'||w_year-1||'52'
	WHERE w_month = 1 AND w_value=52;

	--Reconnecting mismatched weeks. 

	INSERT INTO weekbranch_l0_dummy(l0_id,l0_value,l0_parent,l0_lvl)
	SELECT * FROM l0_dummy; --COPYING VALUES TO NEW TABLE FOR ALTERNATE BRANCH

	FOR yr_cnt IN start_year..end_year LOOP
		FOR q_cnt IN 1..4 LOOP
			INSERT INTO quarter_dummy(q_id, q_value, q_year, q_lvl)
			VALUES (
			'Q_'||yr_cnt||q_cnt, 
			q_cnt, yr_cnt,99); --POPULATING QUARTERS 
		END LOOP;
	END LOOP;

	FOR yr_cnt IN start_year..end_year LOOP
		FOR h_cnt IN 1..2 LOOP
			INSERT INTO h_dummy(h_id, h_value, h_year, h_lvl)
			VALUES (
			'HY_'||yr_cnt||h_cnt, 
			h_cnt,yr_cnt,99); --POPULATING QUARTERS 
		END LOOP;
	END LOOP;

	-- TABLE CREATION END. DATA MAPPING BEGIN

	FOR yr_cnt IN start_year..end_year LOOP 
		UPDATE l1_dummy 
		SET l1_parent=(SELECT l2_id FROM l2_dummy WHERE l2_value=yr_cnt)
		WHERE l1_value_year::INT=((SELECT l2_value FROM l2_dummy WHERE l2_value=yr_cnt));
	END LOOP; --DEFUNCT MONTH YEAR PARENT ASSIGNMENT

	FOR yr_cnt IN start_year..end_year LOOP
		FOR mnth_cnt IN 1..12 LOOP
			UPDATE l0_dummy
			SET l0_parent=(SELECT l1_id FROM l1_dummy WHERE l1_value_month=mnth_cnt AND l1_value_year=yr_cnt)
			WHERE EXTRACT(YEAR FROM l0_value)=yr_cnt AND EXTRACT(MONTH FROM l0_value)=mnth_cnt;
		END LOOP;
	END LOOP; --ASSIGN DAY TO MONTH

	FOR yr_cnt IN start_year..end_year+1 LOOP
		FOR w_cnt IN 1..55 LOOP
			UPDATE weekbranch_l0_dummy
			SET l0_parent=(SELECT DISTINCT w_id FROM w_dummy WHERE w_value=w_cnt AND w_year=yr_cnt)
			WHERE l0_value IN (SELECT DISTINCT w_day FROM w_dummy WHERE w_value=w_cnt AND w_year=yr_cnt);
		END LOOP;
	END LOOP; --ASSIGN DAY TO WEEK

	FOR yr_cnt IN start_year..end_year LOOP
		FOR q_cnt IN 1..4 LOOP
			CASE WHEN q_cnt=1
				THEN
				UPDATE l1_dummy
				SET l1_parent=(SELECT q_id FROM quarter_dummy WHERE q_year=yr_cnt AND q_value=q_cnt)
				WHERE l1_value_year=yr_cnt AND l1_value_month<=3;
				WHEN q_cnt=2
				THEN
				UPDATE l1_dummy
				SET l1_parent=(SELECT q_id FROM quarter_dummy WHERE q_year=yr_cnt AND q_value=q_cnt)
				WHERE l1_value_year=yr_cnt AND l1_value_month<=6 AND l1_value_month>3;
				WHEN q_cnt=3
				THEN
				UPDATE l1_dummy
				SET l1_parent=(SELECT q_id FROM quarter_dummy WHERE q_year=yr_cnt AND q_value=q_cnt)
				WHERE l1_value_year=yr_cnt AND l1_value_month<=9 AND l1_value_month>6;
				WHEN q_cnt=4
				THEN
				UPDATE l1_dummy
				SET l1_parent=(SELECT q_id FROM quarter_dummy WHERE q_year=yr_cnt AND q_value=q_cnt)
				WHERE l1_value_year=yr_cnt AND l1_value_month<=12 AND l1_value_month>9;
			END CASE;
		END LOOP;
	END LOOP; --MONTHS TO HALFYEAR


	FOR yr_cnt IN start_year..end_year LOOP
		FOR q_cnt IN 1..4 LOOP
			CASE WHEN q_cnt=1 OR q_cnt=2 THEN
				UPDATE quarter_dummy
				SET q_parent=(SELECT h_id FROM h_dummy WHERE h_year=yr_cnt AND h_value=1)
				WHERE q_year=yr_cnt AND q_value=q_cnt;
			WHEN q_cnt=3 OR q_cnt=4 THEN
				UPDATE quarter_dummy
				SET q_parent=(SELECT h_id FROM h_dummy WHERE h_year=yr_cnt AND h_value=2)
				WHERE q_year=yr_cnt AND q_value=q_cnt;
			END CASE;
		END LOOP;
	END LOOP; --MAPPING QUARTERS TO HALFYEAR PARENT

	FOR yr_cnt IN start_year..end_year LOOP 
		UPDATE h_dummy 
		SET h_parent=(SELECT l2_id FROM l2_dummy WHERE l2_value=yr_cnt)
		WHERE h_year=((SELECT l2_value FROM l2_dummy WHERE l2_value=yr_cnt));
	END LOOP; --MAPPING HALFYEAR TO YEAR PARENT

	--MAPPING OVER. UNION EVERYTHING
	
	DROP TABLE IF EXISTS new_time_dimension;
	CREATE TABLE new_time_dimension(
	time_combined_id VARCHAR(30) PRIMARY KEY,
	time_id VARCHAR(30),
	time_name VARCHAR(30),
	time_parent VARCHAR(30),
	time_lvl VARCHAR(30),
	timebranch VARCHAR(30)
	);

	INSERT INTO new_time_dimension(time_combined_id, time_id, time_name, time_parent, time_lvl, timebranch)
	SELECT 'M_'||l0_id,
	l0_id, 
	l0_value, 
	l0_parent,
	l0_lvl,
	'main'
	FROM l0_dummy; --INSERTING DAYS

	INSERT INTO new_time_dimension(time_combined_id, time_id, time_name, time_parent, time_lvl, timebranch)
	SELECT 'W_'||l0_id,
	l0_id, 
	l0_value, 
	l0_parent,
	l0_lvl,
	'week'
	FROM weekbranch_l0_dummy; --INSERTING DAYS FOR WEEK BRANCH

	INSERT INTO new_time_dimension(time_combined_id,time_id, time_name, time_parent, time_lvl, timebranch)
	SELECT 'M_'||l1_id,
	l1_id, 
	l1_value_year ||'-'|| l1_value_month, 
	l1_parent,
	l1_lvl,
	'main'
	FROM l1_dummy;

	INSERT INTO new_time_dimension(time_combined_id, time_id, time_name, time_parent, time_lvl, timebranch)
	SELECT 'M_'||l2_id,
	l2_id, 
	l2_value, 
	l2_parent,
	'4',
	'main'
	FROM l2_dummy;

	INSERT INTO new_time_dimension(time_combined_id, time_id, time_name, time_parent, time_lvl, timebranch)
	SELECT 'M_'||q_id,
	q_id, 
	'Q'||q_value||'-'|| q_year, 
	q_parent,
	'2',
	'main'
	FROM quarter_dummy;

	INSERT INTO new_time_dimension(time_combined_id, time_id, time_name, time_parent, time_lvl, timebranch)
	SELECT 
	'M_'||h_id,
	h_id, 
	'HY_'||h_value||'-'|| h_year, 
	h_parent,
	'3',
	'main'
	FROM h_dummy;

	INSERT INTO new_time_dimension(time_combined_id, time_id, time_name, time_parent, time_lvl, timebranch)
	SELECT DISTINCT
	'W_'||w_id,
	w_id, 
	'W_'||w_value||'-'|| w_year, 
	w_parent,
	'1',
	'week'
	FROM w_dummy;

	INSERT INTO time_dimension(time_combined_id, time_id, time_name, time_parent, time_lvl, timebranch)
	SELECT * FROM new_time_dimension
	EXCEPT
	SELECT * FROM time_dimension;

	DROP TABLE IF EXISTS l0_dummy;
	DROP TABLE IF EXISTS weekbranch_l0_dummy;
	DROP TABLE IF EXISTS l1_dummy;
	DROP TABLE IF EXISTS l2_dummy;
	DROP TABLE IF EXISTS quarter_dummy;
	DROP TABLE IF EXISTS h_dummy;
	DROP TABLE IF EXISTS w_dummy;
	DROP TABLE IF EXISTS new_time_dimension;

	RAISE NOTICE 'Finished Update';

	RETURN;
	END;
$BODY$;


