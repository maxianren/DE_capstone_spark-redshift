import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_i94_table_drop = "DROP TABLE IF EXISTS staging_i94_table"
staging_demographics_table_drop = "DROP TABLE IF EXISTS staging_demographics_table"
staging_temperature_table_drop = "DROP TABLE IF EXISTS staging_temperature_table"
traveller_destination_table_drop = "DROP TABLE IF EXISTS traveller_destination_table"
temperature_us_table_drop = "DROP TABLE IF EXISTS temperature_us_table"
demographics_us_table_drop = "DROP TABLE IF EXISTS demographics_us_table"
traveler_table_drop = "DROP TABLE IF EXISTS traveler_table"


# CREATE TABLES

staging_i94_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_i94_table
    (cic_id int,
    i94_year int,
    i94_month int,
    i94_incoming_country text,
    i94_landing_port text,
    arrival_date date,
    i94_travel_mode text,
    i94_address text,
    departure_date date,
    i94_age int,
    i94_visa text,
    match_flag text,
    birth_year int,
    admitted_date date,
    gender text,
    INS_no text,
    airline text,
    admission_no int,
    flight_no text,
    visa_type text
    )
""")

staging_demographics_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_demographics_table
    (
    city text,
    state text,
    state_code text,
    Race text,
    count int,
    male_population int,
    female_population int,
    median_age float,
    total_population int,
    number_of_veterans int,
    foreign_born int,
    average_household_size float
    )
""")


staging_temperature_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_temperature_table
    (
    date date,
    average_temperature float,
    average_temperature_uncertainty float,
    city text,
    latitude text,
    longitude text,
    year int,
    month int,
    state text
    )
""")

traveller_destination_table_create = ("""
CREATE TABLE IF NOT EXISTS traveller_destination_table 
    (cic_id int NOT NULL UNIQUE,
    traveler_INS text,
    traveler_incoming_country text,
    traveler_age int,
    traveler_birth_year int,
    traveler_gender text,
    destination_state text,
    state_avg_temperature float,
    state_total_population int,
    state_median_age float,
    state_foreign_born int,
    state_male_population int,
    state_female_population int

)
""")

temperature_us_table_create = ("""
CREATE TABLE IF NOT EXISTS temperature_us_table 
    (state text NOT NULL UNIQUE,
    average_temperature float,
    average_temperature_uncertainty float
)
""")

demographics_us_table_create = ("""
CREATE TABLE IF NOT EXISTS demographics_us_table 
    (state text NOT NULL UNIQUE,
    median_age float,
    total_population int,
    number_of_veterans int,
    foreign_born int,
    average_household_size float,
    male_population int,
    female_population int
)
""")

traveler_table_create = ("""
CREATE TABLE IF NOT EXISTS traveler_table 
    (cic_id int NOT NULL UNIQUE,
    i94_incoming_country text,
    i94_landing_port text,
    arrival_date date,
    i94_travel_mode text,
    i94_address text,
    departure_date date,
    i94_age int,
    i94_visa text,
    birth_year int,
    admitted_date date,
    gender text,
    INS_no text,
    visa_type text
)
""")


# STAGING TABLES

staging_i94_table_copy = ("""
copy staging_i94_table
from {}
iam_role {}
FORMAT AS PARQUET
""").format(config['PARQUET']['parquet_i94'], config['IAM_ROLE']['ARN'])

staging_demographics_table_copy  = ("""
copy staging_demographics_table
from {}
iam_role {}
FORMAT AS PARQUET
""").format(config['PARQUET']['parquet_dmgr'], config['IAM_ROLE']['ARN'])

staging_temperature_table_copy = ("""
copy staging_temperature_table
from {}
iam_role {}
FORMAT AS PARQUET
""").format(config['PARQUET']['parquet_tmprt'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

temperature_us_table_insert = ("""
INSERT INTO temperature_us_table (
    state,
    average_temperature,
    average_temperature_uncertainty    
    )
SELECT 
    state,
    cast(AVG(average_temperature) as decimal(10,2)) AS average_temperature,
    cast(AVG(average_temperature_uncertainty) as decimal(10,2)) AS average_temperature_uncertainty
FROM staging_temperature_table
WHERE state IS NOT NULL
GROUP BY (state)
""")

demographics_us_table_insert = ("""
INSERT INTO demographics_us_table (
    state,
    median_age,
    total_population,
    number_of_veterans,
    foreign_born,
    average_household_size,
    male_population,
    female_population
)
SELECT 
    state,
    cast(AVG(median_age) as decimal(10,2)) AS median_age,
    SUM(total_population) AS total_population,
    SUM(number_of_veterans) AS number_of_veterans,
    SUM(foreign_born) AS foreign_born,
    cast(AVG(average_household_size) as decimal(10,2)) AS average_household_size,
    SUM(male_population) AS male_population,
    SUM(female_population) AS female_population
FROM staging_demographics_table
WHERE state IS NOT NULL
GROUP BY state
""")

traveler_table_insert = ("""
INSERT INTO traveler_table (
    cic_id,
    i94_incoming_country,
    i94_landing_port,
    arrival_date,
    i94_travel_mode,
    i94_address,
    departure_date,
    i94_age,
    i94_visa,
    birth_year,
    admitted_date,
    gender,
    INS_no,
    visa_type
) 
SELECT 
    cic_id,
    i94_incoming_country,
    i94_landing_port,
    arrival_date,
    i94_travel_mode,
    i94_address,
    departure_date,
    i94_age,
    i94_visa,
    birth_year,
    admitted_date,
    gender,
    INS_no,
    visa_type
FROM staging_i94_table
WHERE cic_id IS NOT NULL
""")

traveller_destination_table_insert = ("""
INSERT INTO traveller_destination_table (
    cic_id,
    traveler_INS,
    traveler_incoming_country,
    traveler_age,
    traveler_birth_year,
    traveler_gender,
    destination_state,
    state_avg_temperature,
    state_total_population,
    state_median_age,
    state_foreign_born,
    state_male_population,
    state_female_population
)
SELECT 
    i.cic_id AS cic_id,
    i.INS_no AS traveler_INS,
    i.i94_incoming_country AS traveler_incoming_country,
    i.i94_age AS traveler_age,
    i.birth_year AS traveler_birth_year,
    i.gender AS traveler_gender,
    i.i94_address AS destination_state,
    t.average_temperature AS state_avg_temperature,
    d.total_population AS state_total_population,
    d.median_age AS state_median_age,
    d.foreign_born AS state_foreign_born,
    d.male_population AS state_male_population,
    d.female_population AS state_female_population
FROM staging_i94_table i 
    LEFT JOIN temperature_us_table t
        ON LOWER(i.i94_address) = LOWER(t.state)
    LEFT JOIN demographics_us_table d 
        ON LOWER(i.i94_address) = LOWER(d.state)
WHERE i.cic_id IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_i94_table_create, staging_demographics_table_create, staging_temperature_table_create, temperature_us_table_create, demographics_us_table_create, traveler_table_create, traveller_destination_table_create]

drop_table_queries = [staging_i94_table_drop, staging_demographics_table_drop, staging_temperature_table_drop, temperature_us_table_drop, demographics_us_table_drop, traveler_table_drop, traveller_destination_table_drop]

copy_table_queries = [staging_i94_table_copy, staging_demographics_table_copy, staging_temperature_table_copy]

insert_table_queries_dim = [temperature_us_table_insert, demographics_us_table_insert, traveler_table_insert]

insert_table_queries_fact = [traveller_destination_table_insert]