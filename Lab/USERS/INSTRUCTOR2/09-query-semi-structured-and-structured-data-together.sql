
-- 9.0.0   Query Semi-Structured and Structured Data Together
--         This lab will take approximately 20 minutes to complete.
--         Snowflake provides native support for semi-structured data. This
--         module will walk you through the steps to:
--         Explore the JSON-formatted historical weather data in the TRAINING_DB
--         database.
--         Query semi-structured data using SQL dot notation and the FLATTEN
--         table function.
--         Create a secure view that joins JSON data to the TRIPS table.
--         The JSON data consists of historical weather station observation data
--         from numerous sources. The information is provided by NOAA Integrated
--         Surface Database (ISD) on the Registry of Open Data on AWS NOAA
--         Integrated Surface Database (ISD). The dataset includes over 35,000
--         stations worldwide, with some having data as far back as 1901, though
--         the data show a substantial increase in volume in the 1940s and again
--         in the early 1970s. This data set has been loaded in JSON format in
--         the database named TRAINING_DB under the WEATHER schema.

-- 9.1.0   Query Weather Data

-- 9.1.1   Set your context.

USE ROLE TRAINING_ROLE;
USE DATABASE INSTRUCTOR2_DB;
USE SCHEMA INSTRUCTOR2_db.public;
USE WAREHOUSE INSTRUCTOR2_QUERY_WH;


-- 9.1.2   Query the contents of the TRAINING_DB.WEATHER.ISD_TOTAL table.

SELECT * FROM TRAINING_DB.WEATHER.ISD_TOTAL LIMIT 500;


-- 9.1.3   Click on one of the records in the result set.
--         The data is stored in raw JSON format
--         Weather data Raw JSON

-- 9.1.4   DESCRIBE the table.

DESCRIBE TABLE TRAINING_DB.WEATHER.ISD_TOTAL;

--         Note that JSON data is stored in the V column with the VARIANT data
--         type.

-- 9.2.0   Query semi-structured data

-- 9.2.1   Query the JSON data in TRAINING_DB.WEATHER.ISD_TOTAL directly using
--         SQL.

SELECT 
       v:data.observations[0].dt::TIMESTAMP_NTZ AS observation_time, 
       v:station.name::STRING AS station,
       v:station.country::STRING AS country,
       v:station.state::STRING AS state,
       v:station.id::STRING AS id,
       v:station.elev::FLOAT AS elevation,
       v:station.coord.lat::FLOAT AS lat,
       v:station.coord.lon::FLOAT AS lon,
       v:station.USAF::STRING AS USAF,
       v:station.WBAN::STRING AS WBAN,
       v:data.observations[0].air.temp::FLOAT AS temp_celsius,
       v:data.observations[0].air."temp-quality-code"::STRING AS temp_qc_code,
       v:data.observations[0].air."dew-point"::FLOAT AS dew_point,
       v:data.observations[0].air."dew-point-quality-code"::STRING AS dew_point_qc_code,
       v:data.observations[0].atmospheric.pressure::FLOAT AS atm_pressure,
       v:data.observations[0].atmospheric."pressure-quality-code"::STRING AS atm_pressure_qc_code,
       v:data.observations[0].sky.ceiling::FLOAT AS sky_ceiling,
       v:data.observations[0].sky."ceiling-quality-code"::STRING AS sky_ceiling_qc_code,
       v:data.observations[0].visibility.distance::FLOAT AS vis_distance,
       v:data.observations[0].visibility."distance-quality-code"::STRING AS vis_distance_qc_code,
       v:data.observations[0].wind."direction-angle"::FLOAT AS wind_direction,
       v:data.observations[0].wind."direction-quality-code"::STRING AS wind_direction_qc_code,   
       v:data.observations[0].wind."speed-quality-code"::STRING AS wind_speed_qc_code,
       v:data.observations[0].wind."speed-rate"::FLOAT AS wind_speed
FROM TRAINING_DB.WEATHER.ISD_TOTAL
WHERE id = '74486094789'
LIMIT 500;

--         You can use SQL dot notation (e.g., station.coord.lat) to extract
--         values from lower levels in the JSON document hierarchy. This allows
--         Snowflake to treat each field as if it were a column in a relational
--         table.
--         In fact, the results look similar to a regular structured data
--         source:
--         JSON Query Results

-- 9.2.2   Query a portion of the data to see the name, country, and ID for
--         stations, as well as the observations array.

SELECT 
     v:station.name::STRING AS station,
     v:station.country::STRING AS country,
     v:station.id::STRING AS id,
     v:data.observations AS observations
FROM TRAINING_DB.WEATHER.ISD_DAILY 
LIMIT 500;

--         OBSERVATIONS is an array with multiple JSON objects.
--         JSON Array Query Results

-- 9.2.3   Use the FLATTEN table function to flatten (explodes) the OBSERVATIONS
--         array into multiple rows.

SELECT weather.t as date, 
       v:station.name::STRING AS station,
       v:station.country::STRING AS country,
       v:station.id::STRING AS id,
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
LIMIT 500;

--         The command above uses the FLATTEN function to extract values into
--         multiple rows. This allows Snowflake to treat each field as if it
--         were a column in a relational table. The command also uses the ::
--         operator to cast values to different data types.
--         The results look similar to a regular structured data source:
--         Flatten JSON Array Query Results

-- 9.3.0   Create a View for Production

-- 9.3.1   Create a view containing weather data from the station at JFK airport
--         in New York (station ID 74486094789).
--         This statement defines a view named WEATHER_NY_VW. This view
--         calculates the maximum and minimum temperatures observed at JFK
--         Airport in New York after 2015-01-01, and converts the air
--         temperature from celsius to fahrenheit.

CREATE OR REPLACE VIEW WEATHER_NY_DEV AS
SELECT weather.t as date, 
       (MAX(observations.value:air.temp) * 9/5 + 32)::NUMBER(38,1) as max_temp_f,
       (MIN(observations.value:air.temp) * 9/5 + 32)::NUMBER(38,1) as min_temp_f
FROM TRAINING_DB.WEATHER.isd_daily weather,
LATERAL FLATTEN(input => v:data.observations) observations 
WHERE observations.value:air."temp-quality-code" = '1'
    AND date >= to_date('2015-01-01') 
    AND weather.v:station.id = '74486094789'
GROUP BY 1;


-- 9.3.2   Query the view to verify that it provides the desired data.

SELECT * FROM WEATHER_NY_DEV
WHERE DATE_TRUNC ('month', date) = '2018-01-01'
LIMIT 50;


-- 9.4.0   Create a Joined View

-- 9.4.1   Run the following statement to create a view joining the WEATHER_VW
--         and TRIPS tables:

CREATE OR REPLACE VIEW TRIP_WEATHER_VW 
    AS SELECT * FROM CITIBIKE.SCHEMA1.TRIPS T
LEFT OUTER JOIN WEATHER_NY_DEV W 
   ON TO_DATE(T.STARTTIME) = TO_DATE(W.DATE);


-- 9.4.2   Use the view to count the total number of trips per day and the
--         maximum and minimum temperatures on that day.

SELECT 
    COUNT(*) AS NUM_TRIPS,
    DATE,
    MAX_TEMP_F,
    MIN_TEMP_F
FROM TRIP_WEATHER_VW
WHERE DATE IS NOT NULL
GROUP BY 2, 3, 4
ORDER BY 1 DESC;

