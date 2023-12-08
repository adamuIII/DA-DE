-- ex.1
--Change from 1.3k to 1300. 
--Change values to int 


SELECT * FROM players
WHERE hits LIKE '%K%';

-- Delete all "K" characters and change values to intigers (1.3k -> 1300)
UPDATE players
SET hits = REPLACE(hits, 'K', '')::numeric * 1000
WHERE hits LIKE '%K%';


SELECT * FROM players
WHERE hits !~ E'^\\d+$';


UPDATE players
SET hits = ROUND(hits::NUMERIC)::INT
WHERE hits ~ E'^\\d+\\.\\d+$';


ALTER TABLE players
ALTER COLUMN hits TYPE INT USING hits::INT;


---------------------------------------
--ex.2
--From feet inch to centimeters
--change from varchar to int
ALTER TABLE players
ADD COLUMN height_copy VARCHAR(250);
UPDATE players
SET height_copy = height;

UPDATE players
SET height_copy = (
  CAST(SPLIT_PART(height, '''', 1) AS INTEGER) * 30.48 + 
  CAST(SPLIT_PART(SPLIT_PART(height_copy, '''', 2), '"', 1) AS INTEGER) * 2.54
)
WHERE height_copy ~ E'^\\d+\'\\d+"$';

UPDATE players
SET height = height_copy

ALTER TABLE players
ALTER COLUMN height TYPE NUMERIC USING height::NUMERIC;

ALTER TABLE players
DROP COLUMN height_copy;

SELECT * FROM players


---------------------------------------
--ex.3
--splitting a column into three different ones
SELECT teamandcontract FROM players

ALTER TABLE players
ADD COLUMN team VARCHAR(250),
ADD COLUMN contractStart VARCHAR(250),
ADD COLUMN contractStop VARCHAR(250);

ALTER TABLE players
DROP COLUMN contractStart;

UPDATE players
SET contractStop = RIGHT(teamandcontract, 6);

select contractStop,teamandcontract from players
UPDATE players 
SET contractStop = REPLACE(contractStop, ' ', '');

UPDATE players
SET contractStop = REPLACE(contractStop, 'Free', '0');

UPDATE players
SET contractStop = REPLACE(contractStop, 'Loan', '0');

UPDATE players
SET contractStop = CAST(contractStop AS INT);
--------------------------
select contractStart from players

UPDATE players
SET contractStart = RIGHT(teamandcontract, 13);

UPDATE players
SET contractStart = LEFT(contractStart, 4);

UPDATE players
SET contractStart = '0'
WHERE contractStart ~ '^[a-zA-Z]+$';

UPDATE players
SET contractStart = REPLACE(contractStart, ' ', '');


UPDATE players
SET contractStart = '0'
WHERE LENGTH(contractStart) != 4;


UPDATE players
SET contractStart = CAST(contractStart AS INT);

-------------------------------------------
UPDATE players
SET team = LEFT(teamandcontract, LENGTH(teamandcontract) - 14)

select team from players

ALTER TABLE players
DROP COLUMN teamandcontract;


------------------------------------- 
----ex.4
---- change value and wage to numeric

UPDATE players
SET wage = REPLACE(wage, '€', '');


UPDATE players
SET wage = REPLACE(wage, 'K', '')::numeric * 1000
WHERE wage LIKE '%K%';

select wage from players
------------------------

ALTER TABLE players
RENAME COLUMN price TO valueIn€;

select valueIn€ from players

update players
SET valueIn€ = REPLACE(valueIn€, '€', '');


-- parsing function
CREATE OR REPLACE FUNCTION parse_value(value_text text) RETURNS integer AS $$
DECLARE
    value_number integer;
BEGIN
    IF value_text ~ '(\d+)K' THEN
        value_number := (CAST(substring(value_text FROM '(\d+)') AS integer)) * 1000;
    ELSIF value_text ~ '(\d+)M' THEN
        value_number := (CAST(substring(value_text FROM '(\d+)') AS integer)) * 1000000;
    ELSE
        value_number := CAST(value_text AS integer);
    END IF;
    RETURN value_number;
END;
$$ LANGUAGE plpgsql;


UPDATE players
SET valueIn€ = parse_value(valueIn€);

ALTER  TABLE players
ALTER COLUMN valueIn€ TYPE INT USING valueIn€::INT;

ALTER  TABLE players
ALTER COLUMN wage TYPE INT USING wage::INT;
-------------------
----ex. 5 
---- lbs to kg
UPDATE players
SET weight = REPLACE(weight, 'lbs', '')::numeric * 0.453
ALTER TABLE players
ALTER COLUMN weight TYPE INT USING hits::INT;


