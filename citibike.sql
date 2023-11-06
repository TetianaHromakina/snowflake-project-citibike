create or replace table trips (
tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);

// Check the content
list @citibike_trips;

// Load data in the staged table
copy into trips from @citibike_trips
file_format=CSV;

// Check the data
select * from trips limit 20;

// Get some sample statistics
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration/60) as "avg duration (min)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1
order by 1;

// Which mongth is the busiest
select monthname(starttime) as "month", count (*) as "num_trips" from trips
group by 1
order by 2 desc;

// create a clone
create table trips_dev clone trips;

// Create a new database
create database weather;

use role accountadmin;
use warehouse compute_wh;
use database weather;
use schema public;

// create a new table
create table json_weather_data (v variant);

// create a new stage
create stage nyc_weather
url = 's3://snowflake-workshop-lab/weather-nyc/';

list @nyc_weather;

copy into json_weather_data
from @nyc_weather
file_format = (type=json);

select * from json_weather_data 
limit 10;

drop view json_weather_data_view;

// create a view
create view json_weather_data_view as
select
v:time::timestamp as observation_time,
v:city.id::int as city_id,
v:city.name::string as city_name,
v:city.country::string as country,
v:city.coord.lat::float as city_lat,
v:city.coord.lon::float as city_lon,
v:clouds.all::int as clouds,
(v:main.temp::float)-273.15 as temp_avg,
(v:main.temp_min::float)-273.15 as temp_min,
(v:main.temp_max::float)-273.15 as temp_max,
v:weather[0].main::string as weather,
v:weather[0].decsription::string as weather_desc,
v:weather[0].icon::string as weather_icon,
v:wind.deg::float as wind_dir,
v:wind.speed::float as wind_speed
from json_weather_data
where city_id = 5128638;

// verify
select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;

// initial correlation between weather and the number of trips 
select weather as conditions,
count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
on date_trunc('hour',observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 
order by 2 desc;

use role accountadmin;
use warehouse compute_wh;
use database citibike;
use schema public;

update trips set start_station_name = 'oops';

select start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

set query_id = 
(select query_id from table(information_schema.query_history_by_session (result_limit=>10))
where query_text like '%update%' order by start_time 
limit 1);

create or replace table trips as
(select * from trips before (statement => '01b02598-0000-2e4b-0000-00000fed4a25'));

use role accountadmin;
create role junior_dba;
grant role junior_dba to user thromakina;
use role junior_dba;

use role accountadmin;
grant usage on database citibike to role junior_dba;
grant usage on database weather to role junior_dba;


