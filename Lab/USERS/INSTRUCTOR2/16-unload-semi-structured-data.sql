
-- 16.0.0  Unload Semi-Structured Data
--         This lab will take approximately 30 minutes to complete.
--         This lab will explore how to unload Semi-Structured Data (JSON and
--         Parquet) from Snowflake tables into a Snowflake stage.

-- 16.1.0  Unload Semi-Structured JSON Data
--         In this exercise, you will unload JSON data that has been loaded into
--         the TRAINING_DB.WEATHER.ISD_2019_TOTAL table. This table contains
--         data of global hourly weather observations compiled and recorded from
--         over 35,000 stations worldwide during the year 2019. Upon completion,
--         you will have unloaded the weather station data to a Snowflake stage
--         separated by each country. In addition, we will explore how to
--         implement a stored procedure to support a dynamic load path to unload
--         the weather data by the country.

-- 16.1.1  Semi Structured Data Types
--         First, explore the global weather data table.

use role training_role;
create warehouse if not exists INSTRUCTOR2_wh;
use warehouse INSTRUCTOR2_wh;
use schema TRAINING_DB.WEATHER;

describe table TRAINING_DB.WEATHER.ISD_2019_TOTAL;

--         The table contains two columns of type VARIANT and TIMESTAMP_NTZ(9).
--         Describe Table

-- 16.1.2  Query the VARIANT column
--         Using Snowflake dot notation, query the v column to view values in
--         TRAINING_DB.WEATHER.ISD_2019_TOTAL for weather stations in the US
--         (country = US).

select v from TRAINING_DB.WEATHER.ISD_2019_TOTAL
where v:station.country = 'US'
limit 10;

--         Click on a row to view the JSON stored in the VARIANT data type; it
--         should look similar to:

-- 16.1.3  Create Unload Objects
--         Now that you have defined the query to return the JSON data, you will
--         use the COPY INTO command to unload the data to a stage.
--         Before running the COPY INTO command, create a STAGE and a named FILE
--         FORMAT.

use warehouse INSTRUCTOR2_wh;
create database if not exists INSTRUCTOR2_db;
use INSTRUCTOR2_db.public;

create or replace stage INSTRUCTOR2_unload;

create or replace file format jsonformat 
  type = 'JSON' COMPRESSION = AUTO;


-- 16.1.4  Use the COPY command to unload JSON data.
--         Use the COPY command to unload all the rows from the
--         TRAINING_DB.WEATHER.ISD_2019_TOTAL table into one or more compressed
--         JSON files in the INSTRUCTOR2_unload stage, with a max file size of 10MB.
--         Also, prefix the unloaded file(s) with json/weather/2019/US/stations
--         to organize the files first by year, and then by country code, in the
--         stage. This will take a few minutes.

copy into @INSTRUCTOR2_unload/json/weather/2019/US/stations from 
(
  select v from TRAINING_DB.WEATHER.ISD_2019_TOTAL
  where v:station.country::string = 'US'
  order by v:station.state
) 
file_format = (format_name = 'jsonformat') max_file_size=1024;


-- 16.1.5  View the list of unloaded files.

list @INSTRUCTOR2_unload/json/weather/2019/US/;

--         Examine the output and verify that path and file size meet the
--         criteria specified in the COPY command.
--         List Unloaded JSON Files

-- 16.2.0  Unload Semi-Structured JSON Data Using a Dynamic Path
--         Now that the weather station data has been unloaded to a Snowflake
--         stage for a single country, the next step is to create a stored
--         procedure to dynamically set the COUNTRY_CODE in the COPY command.

-- 16.2.1  Create a Stored Procedure.

CREATE OR REPLACE PROCEDURE UNLOAD_ISD_2019_TOTAL(COUNTRY_CODE STRING)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
    AS
    $$
     
    var result="";
     
    try {
      var unload_query = "copy into @INSTRUCTOR2_unload/json/weather/2019/" +  
                          COUNTRY_CODE + 
                         "/stations from" +
                         "( select v from TRAINING_DB.WEATHER.ISD_2019_TOTAL" +
                         "  where v:station.country = '" + COUNTRY_CODE + "'" +
                         ") file_format = (type = 'json')" +
                         "  max_file_size=1024" +  
                         "  overwrite=true;";
                             
      var unload_query_results = snowflake.execute({sqlText: unload_query});

      result = "Success: Unloaded " + COUNTRY_CODE + " stations.";
    }
    catch (err)  {
        result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
        result += "\n  Message: " + err.message;
        result += "\nStack Trace:\n" + err.stackTraceTxt; 
    }
       
    return result;
    $$;

--         Examine the body of the stored procedure and note that the COPY INTO
--         statement is nearly the same as that last step. The only modification
--         was to add the overwrite=true option.
--         Next, invoke the UNLOAD_ISD_2019_TOTAL procedure, passing in the
--         COUNTRY_CODE for the US.

CALL UNLOAD_ISD_2019_TOTAL('US');

LIST @INSTRUCTOR2_unload/json/weather/2019/US/;

--         Confirm the number of rows from the results of the LIST command.
--         A Snowflake stored procedure can include procedural logic (branching
--         and looping), which straight SQL does not support. This can provide
--         error handling, and dynamically create a SQL statement and execute
--         it.

-- 16.2.2  Get a list of country codes.
--         Run the following query to find the list of distinct country codes in
--         the TRAINING_DB.WEATHER.ISD_2019_TOTAL table.

select DISTINCT(v:station.country::string) from TRAINING_DB.WEATHER.ISD_2019_TOTAL;

--         There are hundreds of country codes in the data. On your own time,
--         try to rewrite the stored procedure to run the provided query to get
--         a LIST OF COUNTRY CODES, then use a loop to iterate through the list,
--         running a COPY INTO statement for each COUNTRY CODE.

-- 16.3.0  Unload Structured Data to a JSON File
--         In this exercise, you will unload the data in the relational table
--         SNOWSTORE.DWH.DIMCUSTOMERS into a JSON file that meets the specified
--         JSON syntax for storing and exchanging data with a third party
--         vendor.
--         Below is a JSON document that meets the required specification:

/*
{
  "customer_info": {
    "key": 118,
    "id": 386,
    "city": "Gold Coast",
    "state": "Queensland"
    "country": "Australia"
  },
  "customer_pii": {
    "firstname": "Refugio ",
    "lastname": "Whittaker "
    "gender": "M",
    "latlong": [ 0, 0]
  }
}
*/

--         You will use the VARIANT, OBJECT and ARRAY data types to transform
--         and unload the structured data source into the JSON document that
--         meets the required specification.

-- 16.3.1  Query the structured table.
--         Explore the columns and values of the SNOWSTORE.DWH.DIMCUSTOMERS
--         table to determine what structuced columns will need to be mapped to
--         the JSON document specification.

select * from SNOWSTORE.DWH.DIMCUSTOMERS;

describe table SNOWSTORE.DWH.DIMCUSTOMERS;

--         Looking at the columns names, we can apply the following mapping
--         rules:

/*
| JSON key name | Table Column Name |
|---------------|-------------------|
| key           | CUSTOMERKEY       |
| id            | CUSTOMERID        |
| city          | CITY              |
| state         | STATE             |
| country       | COUNTRY           |
| firstname     | FIRSTNAME         |
| lastname      | LASTNAME          |
| gender        | GENDER            |
| latlong       | NO MATCH          |
*/


-- 16.3.2  Create the JSON objects.
--         Use the semi-structured data function OBJECT_CONSTRUCT to create the
--         objects (key:value pairs) with the corresponding values in
--         SNOWSTORE.DWH.DIMCUSTOMERS table.

select object_construct('key', CUSTOMERKEY,
                        'id', CUSTOMERID,
                        'city', CITY,
                        'state', STATE,
                        'country', COUNTRY,
                        'firstname', FIRSTNAME,
                        'lastname', LASTNAME,
                        'gender', GENDER)                  
from SNOWSTORE.DWH.DIMCUSTOMERS;

--         Examine the output, we are getting close. The output looks something
--         like that shown below. Note, we do not need to be concerned about
--         ordering of the keys only that the keys match the specification.

/*
{
  "city": "Gold Coast",
  "country": "Australia",
  "firstname": "Refugio ",
  "gender": "Male",
  "id": 386,
  "key": 118,
  "lastname": "Whittaker ",
  "state": "Queensland"
}
*/


-- 16.3.3  Create nested JSON objects.
--         Next, create and add the objects (key:value pairs) to the
--         customer_info and customer_pii objects.

select object_construct(
          'customer_info',object_construct(
                            'key',CUSTOMERKEY,
                            'id',CUSTOMERID,
                            'city',CITY,
                            'state',STATE,
                            'country',COUNTRY),
          'customer_pii',object_construct(
                            'firstname',FIRSTNAME,
                            'lastname',LASTNAME,
                            'gender',GENDER)
)                 
from SNOWSTORE.DWH.DIMCUSTOMERS;

--         Examine the output. All that is left is to create the latlong array.

/*
{
  "customer_info": {
    "city": "Gold Coast",
    "country": "Australia",
    "id": 386,
    "key": 118,
    "state": "Queensland"
  },
  "customer_pii": {
    "firstname": "Refugio ",
    "gender": "Male",
    "lastname": "Whittaker  "
  }
}
*/


-- 16.3.4  Add the latlong array.
--         Create the latlong object and use the array_construct method with
--         values of 0.0, since the structured data does not contain that
--         information.

select object_construct(
          'customer_info',object_construct(
                            'key',CUSTOMERKEY,
                            'id',CUSTOMERID,
                            'city',CITY,
                            'state',STATE,
                            'country',COUNTRY),
          'customer_pii',object_construct(
                            'firstname',FIRSTNAME,
                            'lastname',LASTNAME,
                            'gender',GENDER,
                            'latlong',array_construct(0.0,0.0))
)                 
from SNOWSTORE.DWH.DIMCUSTOMERS;

--         Examine the output. Notice that there is some unwanted whitespace in
--         some of the string values and that the gender value should be a
--         single upper case letter.

/*
{
  "customer_info": {
    "city": "Gold Coast",
    "country": "Australia",
    "id": 386,
    "key": 118,
    "state": "Queensland"
  },
  "customer_pii": {
    "LATLONG": [
      0,
      0
    ],
    "firstname": "Refugio ",
    "gender": "Male",
    "lastname": "Whittaker "
  }
}
*/


-- 16.3.5  Clean up the final output.
--         Use the TRIM and REPLACE functions to remove the unwanted whitespace
--         for the columns that have VARCHAR types and replace the gender value
--         with the correct abbreviation.

select object_construct(
          'customer_info',object_construct(
                            'key',CUSTOMERKEY,
                            'id',CUSTOMERID,
                            'city',TRIM(CITY),
                            'state',TRIM(STATE),
                            'country',TRIM(COUNTRY)),
          'customer_pii',object_construct(
                            'firstname',TRIM(FIRSTNAME),
                            'lastname',TRIM(LASTNAME),
                            'gender',case 
                               when UPPER(TRIM(GENDER)) = 'MALE' then 'M'
                               when UPPER(TRIM(GENDER)) = 'FEMALE' then 'F'
                               else 'O' end,
                            'LATLONG',array_construct(0.0,0.0))
)                 
from SNOWSTORE.DWH.DIMCUSTOMERS;


-- 16.3.6  Unload the JSON data.
--         Now that the select statement is complete, add it to the COPY INTO
--         command:

copy into @INSTRUCTOR2_unload/json/customers from 
(
  select object_construct(
          'customer_info',object_construct(
                            'key',TRIM(CUSTOMERKEY),
                            'id',TRIM(CUSTOMERID),
                            'city',TRIM(CITY),
                            'state',TRIM(STATE),
                            'country',TRIM(COUNTRY)),
          'customer_pii',object_construct(
                            'firstname',TRIM(FIRSTNAME),
                            'lastname',TRIM(LASTNAME),
                            'gender',case 
                               when UPPER(TRIM(GENDER)) = 'MALE' then 'M'
                               when UPPER(TRIM(GENDER)) = 'FEMALE' then 'F'
                               else 'O' end,
                            'LATLONG',array_construct(0.0,0.0))
  )                 
  from SNOWSTORE.DWH.DIMCUSTOMERS
) 
file_format = (format_name = 'jsonformat');


-- 16.3.7  List the file(s) generated.

list @INSTRUCTOR2_unload/json/customers;


-- 16.3.8  Query the JSON file from the stage.

select metadata$filename, metadata$file_row_number, $1 
from @INSTRUCTOR2_unload/json/customers_0_0_0.json.gz 
(file_format => jsonformat);


-- 16.4.0  Unload Semi-Structured Parquet Data
--         In this exercise, you will unload JSON data in the
--         TRAINING_DB.WEATHER.ISD_DAILY table in Parquet format, to a Snowflake
--         named stage organized by each country.
--         This table contains global hourly weather observation data compiled
--         and recorded from over 35,000 stations worldwide during the year from
--         1901 to 2019. Since Parquet is a column-oriented format, you will
--         transform the semi-structured JSON data into a structured format.
--         Upon completion, you will have unloaded the weather station data to a
--         Snowflake stage for a single country for the year 2019.

-- 16.4.1  Create Unload Objects.
--         Before creating the COPY INTO command, create a named FILE FORMAT
--         object.

create or replace file format parquetformat 
  type = 'PARQUET' COMPRESSION = AUTO;


-- 16.4.2  Query a VARIANT column.

select v from TRAINING_DB.WEATHER.ISD_2019_DAILY limit 25;

--         Examine the results. Notice how the observations array contains
--         station measurements for each hour of the day.

-- 16.4.3  FLATTEN the output.
--         Use the FLATTEN function, a LATERAL join, and dot notation to create
--         a row for each station’s observations array.

 SELECT weather.t as date, 
       v:station.name::STRING AS station,
       v:station.country::STRING AS country,
       v:station.id::STRING AS id,
       observations.value:dt::timestamp AS time,
       observations.value:air.temp::FLOAT AS temp_celsius,
       observations.value:air."temp-quality-code"::STRING AS temp_qc_code,
       observations.value:air."dew-point"::FLOAT AS dew_point,
       observations.value:air."dew-point-quality-code"::STRING AS dew_point_qc_code,
       observations.value:atmospheric.pressure::FLOAT AS atm_pressure,
       observations.value:atmospheric."pressure-quality-code"::STRING AS atm_pressure_qc_code,
       observations.value:sky.ceiling::FLOAT AS sky_ceiling,
       observations.value:sky."ceiling-quality-code"::STRING AS sky_ceiling_qc_code,
       observations.value:visibility.distance::FLOAT AS vis_distance,
       observations.value:visibility."distance-quality-code"::STRING AS vis_distance_qc_code,
       observations.value:wind."direction-angle"::FLOAT AS wind_direction,
       observations.value:wind."direction-quality-code"::STRING AS wind_direction_qc_code,   
       observations.value:wind."speed-quality-code"::STRING AS wind_speed_qc_code,
       observations.value:wind."speed-rate"::FLOAT AS wind_speed
FROM TRAINING_DB.WEATHER.isd_daily weather,
LATERAL FLATTEN(input => v:data.observations) observations
WHERE date BETWEEN to_timestamp_ntz('2019-01-01 00:00:00') AND to_timestamp_ntz('2019-12-31 23:59:59') 
AND country = 'US'
LIMIT 500;

--         Examine the results. There is one row for each measurement in each
--         station.
--         Query Results

-- 16.4.4  Unload the structured data to a Parquet file.
--         Use COPY INTO to unload the transformed JSON data to Parquet. To
--         retain the column names in the output file, use the HEADER = TRUE
--         option.

copy into @INSTRUCTOR2_unload/parquet/weather/2019/US/stations from 
(
  SELECT weather.t as date, 
       v:station.name::STRING AS station,
       v:station.country::STRING AS country,
       v:station.id::STRING AS id,
       observations.value:dt::timestamp AS time,
       observations.value:air.temp::FLOAT AS temp_celsius,
       observations.value:air."temp-quality-code"::STRING AS temp_qc_code,
       observations.value:air."dew-point"::FLOAT AS dew_point,
       observations.value:air."dew-point-quality-code"::STRING AS dew_point_qc_code,
       observations.value:atmospheric.pressure::FLOAT AS atm_pressure,
       observations.value:atmospheric."pressure-quality-code"::STRING AS atm_pressure_qc_code,
       observations.value:sky.ceiling::FLOAT AS sky_ceiling,
       observations.value:sky."ceiling-quality-code"::STRING AS sky_ceiling_qc_code,
       observations.value:visibility.distance::FLOAT AS vis_distance,
       observations.value:visibility."distance-quality-code"::STRING AS vis_distance_qc_code,
       observations.value:wind."direction-angle"::FLOAT AS wind_direction,
       observations.value:wind."direction-quality-code"::STRING AS wind_direction_qc_code,   
       observations.value:wind."speed-quality-code"::STRING AS wind_speed_qc_code,
       observations.value:wind."speed-rate"::FLOAT AS wind_speed
FROM TRAINING_DB.WEATHER.isd_daily weather,
LATERAL FLATTEN(input => v:data.observations) observations
WHERE date BETWEEN to_timestamp_ntz('2019-01-01 00:00:00') AND to_timestamp_ntz('2019-12-31 23:59:59')
AND country = 'US'
  
) 
file_format = (format_name = 'parquetformat')
OVERWRITE = TRUE 
HEADER = TRUE;


-- 16.4.5  List the files in the stage.

list @INSTRUCTOR2_unload/parquet/weather/2019/US/stations;


-- 16.4.6  Query the PARQUET file in the stage.

select $1 from @INSTRUCTOR2_unload/parquet/weather/2019/US/stations_0_0_0.snappy.parquet (file_format => parquetformat);

