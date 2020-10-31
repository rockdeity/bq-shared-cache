#standardSQL
-- triple_z_error
with settings as (
  SELECT
    2019 as start_year
    ,2021 as end_year
),

planning_date_dim_table as (
  select * FROM settings
  --order by 1
),

planning_week_dim_table as (
  select *
  from planning_date_dim_table
),

weeks as (
  select * FROM planning_week_dim_table
),

filtered_oil_table as (
  SELECT
  *
  FROM settings cross join planning_date_dim_table as d
),

-- with dates as ( -- wsn with week start and end dates
--   select wd.wsn,
--   dd.dt,
--   wd.iso_year,
--   wd.dt as ds,
--   wd.cy_07d_end as ds_end
--   from `{planning_date_dim_table}` as dd
--   join `{planning_week_dim_table}` as wd
--     on dd.dt between wd.dt and wd.cy_07d_end
--   where wd.iso_year between {start_year} and {end_year}
-- )

offering as (
  SELECT *
  from weeks
),

offering_historical as (
  SELECT *
  from weeks
),

weeks_offered as (
  SELECT *
  FROM offering_historical
),

sales_fact as (
  select *
  from filtered_oil_table as oil
  cross join weeks
),

latest_forecasts AS (
  -- latest prediction for each ds for each product
  SELECT * FROM weeks w cross JOIN sales_fact
),


cached_week_grouped_forecast_and_sales as (
  SELECT *
  FROM latest_forecasts
  CROSS JOIN sales_fact
)


SELECT * FROM cached_week_grouped_forecast_and_sales

