# weather-data-processor
This repository will be used for data ingestion & processing of National Centers for Environmental Information data.

-----

# Synopsis

    
    The Exercise may require a few tens of GB of space. The exercises can be submitted as a zip archive of the source directory. 

    ** Part 1:**

    Take the following files, create a database structure for them and load them into PostgreSQL. 
    
    Whether you normalize the data is up to you, but both header and record data must be stored. You can use any language on the JVM, 
    
    but without using Spark or ORM-like libraries.  

     

    The structure is documented at 

    https://www1.ncdc.noaa.gov/pub/data/igra/data/igra2-data-format.txt

    https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por/USM00070261-data.txt.zip

    https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por/USM00070219-data.txt.zip

    https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por/USM00070361-data.txt.zip

    https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por/USM00070308-data.txt.zip

    https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por/USM00070398-data.txt.zip

     

    **Part 2: **
    

    In Spark, connect to Postgres, digest the data, and write Apache Parquet files.
    
    The parquet files should include all data for the weather balloons, 
    
    and be partitioned by thousands of meters altitude (i.e. partitioning to 0-1000, 1001-2000, 2001-3000, etc).
    
    You may create multiple files per partition but each file should only contain data for one partition.

-----

# Requirements

    JDK 1.7 or higher
    maven
    Scala 2.11.11
    Spark 2.3.3
    PostgreSQL 10 
    *AWS Lambda
    *AWS EMR

    * If we want to run the job on AWS EMR Using AWS Lambda.
----

# Build
    
    *** Change your /config/pipeline_config.json entries 
    according to your configuration ***

    Go to project root dir
    > mvn clean install -U

------

# Run

    Can use /script/lambda/amazon_review_lambda.py to configure a lambda function on AWS lambda
    by changing your VPC & security grougp configuration.

    Or For dry run execute SoundDataIngestor scala class. 
    Then to run spark data loader call WeatherDataController with "DEV" OR "PROD" as runtime parameter.

------

# Postgres Table Creation

    CREATE TABLE weather_header(
    HEADER_ID SERIAL,
    ID  CHAR(12)    NOT NULL,
    YEAR           INT    NOT NULL,
    MONTH            INT     NOT NULL,
    DAY        INT     NOT NULL,
    HOUR        INT     NOT NULL,
    RELTIME    INT     NOT NULL,
    NUMLEV INT ,
    P_SRC CHAR(10) ,
    NP_SRC CHAR(10),
    LAT INT,
    LON INT,
    UID CHAR(60) PRIMARY KEY     NOT NULL
    );
    
    
    CREATE TABLE IF NOT EXISTS public.weather_data(
    LVLTYP1           INT   ,
    LVLTYP2            INT    ,
    ETIME        INT    ,
    PRESS        INT   ,
    PFLAG CHAR(3),
    GPH    INT   ,
    ZFLAG CHAR(3),
    TEMP INT ,
    TFLAG CHAR(3),
    RH INT ,
    DPDP INT,
    WDIR INT,
    WSPD INT,
    UID CHAR(60)   NOT NULL
    );

    CREATE TABLE IF NOT EXISTS etl_monitor(
    ID SERIAL PRIMARY KEY,
    ETL_NAME CHAR(60) NOT NULL,
    SKIP_EXECUTION CHAR(1) NOT NULL,
    FULL_LOAD CHAR(1) NOT NULL,
    run_date timestamp,
    filter_col1_name VARCHAR(100),
    filter_col1_value  VARCHAR(1000),
    filter_col2_name VARCHAR(100),
    filter_col2_value VARCHAR(1000)
    );

    INSERT INTO etl_monitor (ETL_NAME , SKIP_EXECUTION, FULL_LOAD, run_date, filter_col1_name)
    VALUES ('weather-data', 'N', 'Y', current_timestamp, 'HEADER_ID');

----
