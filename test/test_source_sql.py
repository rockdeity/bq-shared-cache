complex_query = """
    #standardSQL
    -- triple_z_error
    with Settings as (
      SELECT
        2019 as start_year
        ,2021 as end_year
    )   ,
    
    planning_date_dim_table as (
      select
        q1.*
        ,dense_rank() over (order by iso_year) as ysn -- year sequence number
        ,dense_rank() over (order by iso_week_id) as wsn -- week sequence number
        ,row_number() over (order by dt) as dsn -- day sequence number
        ,is_mon+is_tue+is_wed+is_thu+is_fri as is_weekday
        ,is_sat+is_sun as is_weekend
      from (
        select q0.*
          ,case when dow = 1 then 1 else 0 end as is_mon
          ,case when dow = 2 then 1 else 0 end as is_tue
          ,case when dow = 3 then 1 else 0 end as is_wed
          ,case when dow = 4 then 1 else 0 end as is_thu
          ,case when dow = 5 then 1 else 0 end as is_fri
          ,case when dow = 6 then 1 else 0 end as is_sat
          ,case when dow = 7 then 1 else 0 end as is_sun
          ,(iso_year*100)+iso_week as iso_week_id
        from (
          select dt
            ,cast(dt as timestamp) as ts
            ,extract(year from dt) as cal_year
            ,extract(month from dt) as cal_month
            ,extract(day from dt) as cal_day
            ,mod(extract(dayofweek from dt)+5,7)+1 as dow
            ,format_date('%a', dt) as dow_abbr
            ,extract(isoyear from dt) as iso_year
            ,extract(isoweek from dt) as iso_week
          from (
            select *
            from unnest(generate_date_array(
            '2010-01-25' -- Monday preceeding Zulily going live on 2010-01-27
            ,CAST('3000-01-01' as DATE), interval 1 day)) as dt
          )
      ) as q0
      ) as q1
      --order by 1
    ),
    
    planning_week_dim_table as (
      select
        wsn
        ,iso_year
        ,dt
        ,cast(date_add(dt,interval ( 7-1) day) as Date) as cy_07d_end
        ,cast(date_add(dt,interval (14-1) day) as Date) as cy_14d_end
        ,date_add(dt,interval (28-1) day) as cy_28d_end
        ,date_add(dt,interval (63-1) day) as cy_63d_end
        ,date_add(dt,interval (91-1) day) as cy_91d_end
        ,date_sub(dt,interval 364 day) as ly_beg
        ,date_sub(dt,interval (364- 7+1) day) as ly_07d_end
        ,date_sub(dt,interval (364-14+1) day) as ly_14d_end
        ,date_sub(dt,interval (364-28+1) day) as ly_28d_end
        ,date_sub(dt,interval (364-63+1) day) as ly_63d_end
        ,date_sub(dt,interval (364-91+1) day) as ly_91d_end
        ,date_add(dt,interval 364 day) as ny_beg
        ,date_add(dt,interval (364+ 7-1) day) as ny_07d_end
        ,date_add(dt,interval (364+14-1) day) as ny_14d_end
        ,date_add(dt,interval (364+28-1) day) as ny_28d_end
        ,date_add(dt,interval (364+63-1) day) as ny_63d_end
        ,date_add(dt,interval (364+91-1) day) as ny_91d_end
      from planning_date_dim_table
      where is_mon = 1
    ),
    
    weeks as (
      select 
        wsn,
        iso_year,
        dt as ds,
        cy_07d_end as ds_week_end_dt
      from planning_week_dim_table
    ),
    
    filtered_oil_table as (
      SELECT
      *,
      if(has_zps_item_tag + has_zps_tracking_number + has_is_zps > 0, 1, 0) as is_zps_sale
      FROM(
      select
      d.wsn,
      o.order_date,
      o.product_style_id,
      o.product_id,
      o.customer_id,
      o.sold_in_event_id,
      round(o.demand_amount_usd,2) as demand,
      round(o.discount_amount_usd,2) as discount,
      if(REGEXP_CONTAINS(ifnull(o.item_tags, 'X'), '(?i)zps'), 1, 0) as has_zps_item_tag,
      if(REGEXP_CONTAINS(ifnull(o.ib_tracking_number, 'X'), '(?i)zps'), 1, 0) as has_zps_tracking_number,
      if(ifnull(o.is_zps, -1) = 1, 1, 0) as has_is_zps,
      1 as units
      from zulilymarketing.rpt_datamart.order_item_lifecycle as o
      CROSS JOIN settings
      join planning_date_dim_table as d
      on o.order_date = d.dt
      and LOWER(o.item_type) = 'physical'
      AND dist_center_id in (8, 11, 12, 14, 15, 16, 17) # Exclude dropship
      -- These filters are not required because we want to estimate demand and not consider cancelled order.
      -- and o.cancel_timestamp IS NULL
      -- and LOWER(o.application) != 'zucare'
      and o.product_style_id is not null
      and o.product_id is not null
      and d.iso_year between start_year and end_year
      )
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
      SELECT distinct 
      wsn,
      iso_year,
      ds,
    --   ds_end,
      product_style_id,
      product_id
      from weeks
      join (
        SELECT * 
        FROM
          `stone-outpost-636.datamart.product_offering_fact`
        WHERE DATE(event_start_date) >= '2020-08-01' -- AND DATE(event_start_date)  <= '2020-08-14'
      ) as pof
      on ds between date(event_start_date) and date(event_end_date)
    ),
    
    offering_historical as (
      SELECT distinct 
      wsn,
      iso_year,
      ds,
    --   ds_end,
      product_style_id,
      product_id
      from weeks
      join (
        SELECT * 
        FROM
          `stone-outpost-636.datamart.product_offering_fact`
        WHERE DATE(event_start_date) >= '2018-08-01' -- AND DATE(event_start_date)  <= '2020-08-14'
      ) as pof
      on ds between date(event_start_date) and date(event_end_date)
    ),
    
    weeks_offered as (
      SELECT 
        product_style_id,
        product_id,
        count(*) as count,
        CAST(count(*) / 10 as int64) * 10 as bracket
      FROM offering_historical
      GROUP BY
        product_style_id,
        product_id
    ),
    
    event_division AS (
    SELECT eventId event_id, divisionId, d.divisionName ,d.shortName
      FROM `stone-outpost-636.tier1_merch.event` e
      INNER JOIN
      `stone-outpost-636.tier1_merch.division` d using(divisionId)
    ),
    
    sales_fact as (
      select
      w.wsn,
      w.iso_year,
      w.ds,
      w.ds_week_end_dt as ds_end,
      oil.product_style_id,
      oil.product_id,
      --ed.divisionId,
      --ed.shortName,
      sum(oil.units) as units,
      sum(oil.units * oil.is_zps_sale) as zps_units,
      sum(oil.units * (1 - oil.is_zps_sale)) as vs_units
      from filtered_oil_table as oil
    join weeks w using (wsn)
    --LEFT JOIN event_division ed on ed.event_id = oil.sold_in_event_id
    group by w.wsn, w.iso_year, w.ds, w.ds_week_end_dt, oil.product_style_id, oil.product_id --, ed.divisionId, ed.shortName
    ),
    
    latest_forecasts AS (
      -- latest prediction for each ds for each product
      SELECT
        DATE(forecast_week_target_ds) forecast_week_target_date,
        DATE(forecast_week_source_ds) forecast_week_source_date,
        product_id,
        product_style_id,
        wsn - hld_beg_wsn as lookahead_weeks,
        (wsn - hld_beg_wsn) < 4 as is_4_week,
        yhat as forecast_weekly_sales,
        root_run_timestamp as data_pull_time,
        wsn,
        snaive_units,
        ok_for_mase,
        cohort,
        hld_beg_wsn
      FROM (
        SELECT
          forecasts.ds as forecast_week_target_ds,
          w.ds as forecast_week_source_ds,
          forecasts.product_style_id,
          forecasts.product_id,
          hld_beg_wsn,
          yhat, --IF(yhat < 0, 0, yhat) as yhat,
          root_run_timestamp,
          forecasts.wsn as wsn,
          forecasts.snaive_units, --ly_sales.snaive_units,
          ok_for_mase,
          cohort,
        ROW_NUMBER() OVER (
          PARTITION BY
            forecasts.product_id,
            forecasts.product_style_id,
            hld_beg_wsn,
            forecasts.ds
        ORDER BY
          root_run_timestamp DESC
        ) AS rnk
        FROM
          `massive-clone-705.triple_z_demand_prediction_prod_2.predictions_v7_product_expire` as forecasts
          --`massive-clone-705.triple_z_demand_prediction.predictions_v7_product`
        JOIN weeks w ON 
          hld_beg_wsn = w.wsn -- get dates info from forecast source
         JOIN ( 
          SELECT
            product_style_id,
            product_id,
            wsn,
            units as snaive_units
          FROM sales_fact
        ) ly_sales
        ON
          ly_sales.product_style_id = forecasts.product_style_id AND
          ly_sales.product_id = forecasts.product_id AND
          ly_sales.wsn = w.wsn - 52 
    
        WHERE 1=1
          AND CAST(forecasts.ds as DATE) <= DATE_SUB(EXTRACT(DATE FROM CURRENT_TIMESTAMP()), INTERVAL 7 DAY) -- give time for sales figures to settle
          AND CAST(forecasts.ds as DATE) >=  '2020-07-03'
          AND cohort in  ('20541', '18583', '5283', '68132', '14358', '12996', '22533', '14548', '16769', '13026')
          AND ly_sales.snaive_units IS NOT NULL AND ly_sales.snaive_units > 0
          AND CAST(w.ds as DATE) <= DATE_SUB(EXTRACT(DATE FROM CURRENT_TIMESTAMP()), INTERVAL 9 WEEK)
          --AND root_run_commit = '6490a05fb43b86a1ec0c81a435b7406385a40962-rmartin-pilot-rand' -- original  3 pilot vendors
          --AND root_run_commit =  '48c1a83e2bfcbb9119d025d70e4418780f4d5f02-rmartin-pilot-rand' -- new pilot vendor candidates
          --AND root_run_commit  in('6490a05fb43b86a1ec0c81a435b7406385a40962-rmartin-pilot-rand', '48c1a83e2bfcbb9119d025d70e4418780f4d5f02-rmartin-pilot-rand')
        )
        WHERE
        rnk = 1
    ),
    
    
    week_grouped_forecast_and_sales as (
      SELECT
        product_style_id,
        product_id,
        SUM(forecast_weekly_sales) as week_grouped_forecast_sales,
        SUM(IFNULL(units, 0)) as week_grouped_units,
        SUM(IFNULL(zps_units, 0)) as week_zps_units,
        SUM(IFNULL(snaive_units, 0)) as week_snaive_units,
        --ok_for_mase,
        cohort,
        bracket--,
    --     divisionId,
    --     shortName,
        -- lookahead_weeks,  -- uncomment to look week-by-week
    --     forecast_week_target_date,  -- uncomment to look week-by-week
        --forecast_week_source_date
        --extract (YEAR FROM forecast_week_target_date) as forecast_year,
        --extract (MONTH from forecast_week_target_date) as forecast_month,
        --wsn
      FROM latest_forecasts
      --JOIN offering using (wsn, product_style_id, product_id)
      --JOIN weeks_offered using (product_style_id, product_id)
      LEFT JOIN (
        SELECT 
          *,
          CAST(IFNULL(units, 0) / 10 as int64) * 10 as bracket
        FROM sales_fact
      )
      USING (product_style_id, product_id, wsn)
      WHERE 1=1
        AND snaive_units is not null and snaive_units > 0
    --     AND ok_for_mase != 0
      GROUP BY
        product_style_id,
        product_id,
        --ok_for_mase,
        cohort,
        bracket--,
    --     divisionId,
    --     shortName,
        --wsn,
        -- lookahead_weeks,  -- uncomment to look week-by-week
    --     forecast_week_target_date, -- uncomment to look week-by-week
        --forecast_week_source_date
    )
    
    -- debug latest_forecasts
    -- SELECT * FROM (
    --   SELECT 
    --     count(*) as count,
    --     product_style_id,
    --     product_id,
    --   --   lookahead_weeks,
    --   --   forecast_week_source_date,
    --     forecast_week_target_date,
    --     SUM(ok_for_mase) as ok_for_mase
    --   FROM latest_forecasts
    --   GROUP BY
    --     product_style_id,
    --     product_id,
    --   --   lookahead_weeks,
    --   --   forecast_week_source_date,
    --     forecast_week_target_date--,
    --     --ok_for_mase
    --   ORDER BY
    --     count(*) DESC,
    --     product_style_id,
    --     product_id,
    --   --   lookahead_weeks,
    --   --   forecast_week_source_date,
    --     forecast_week_target_date,
    --     ok_for_mase
    -- ) WHERE ok_for_mase != 0 and ok_for_mase != count
    
    -- debug aggregated sales
    -- SELECT 
    --   count(*) as count,
    --   product_style_id,
    --   product_id,
    -- --   units,
    --   ds
    -- FROM sales_fact
    -- GROUP BY
    --   product_style_id,
    --   product_id,
    -- --   units,
    --   ds
    -- ORDER BY
    --   count(*) DESC,
    --   product_style_id,
    --   product_id,
    --   ds
    
    -- debug aggregated forecasts with sales
    -- SELECT 
    --   count(*) as count,
    --   product_style_id,
    --   product_id,
    --   week_grouped_units,
    --   forecast_week_source_date--,
    --   --forecast_week_target_date,
    --  -- ok_for_mase
    -- FROM week_grouped_forecast_and_sales
    -- GROUP BY
    --   product_style_id,
    --   product_id,
    --   week_grouped_units,
    --   forecast_week_source_date--,
    --   --forecast_week_target_date,
    -- --   ok_for_mase
    -- ORDER BY
    --   count(*) DESC,
    --   product_style_id,
    --   product_id,
    --   forecast_week_source_date--,
    --   --forecast_week_target_date,
    --   --ok_for_mase
    
    -- debug aggregated forecasts with offering
    -- SELECT 
    --   count(*) as count,
    --   product_style_id,
    --   product_id,
    --   --week_grouped_units,
    --   --forecast_week_source_date,
    --   forecast_week_target_date
    --  -- ok_for_mase
    -- FROM week_grouped_forecast_and_sales 
    -- JOIN offering using (wsn, product_style_id, product_id)
    -- GROUP BY
    --   product_style_id,
    --   product_id,
    --   --week_grouped_units,
    --   --forecast_week_source_date,
    --   forecast_week_target_date
    -- --   ok_for_mase
    -- ORDER BY
    --   count(*) DESC,
    --   product_style_id,
    --   product_id,
    --   --forecast_week_source_date,
    --   forecast_week_target_date
    --   --ok_for_mase
    
    SELECT 
      ROUND(SAFE_DIVIDE(improvement_units_total, total_units), 2) as improvementPerSale_total,
      ROUND(SAFE_DIVIDE(improvement_units_overstock, total_units), 2) as improvementPerSale_Overstock,
      ROUND(SAFE_DIVIDE(improvement_units_understock, total_units), 2) as improvementPerSale_Understock,
      *
    FROM (
    
      SELECT
        ROUND(SAFE_DIVIDE(zps_total_units, total_units) * 100, 2) as zps_units_percentage,
        ROUND(SAFE_DIVIDE(total_abs_error, total_units), 2) as AbsErrorPerSales,
        ROUND(SAFE_DIVIDE(total_abs_error_snaive, total_units), 2) as AbsErrorPerSales_snaive,
        ROUND(SAFE_DIVIDE(total_abs_error, total_abs_error_snaive), 2) ABSErrorPerSNaive,
        CAST(total_abs_error_snaive - total_abs_error as int64) as improvement_units_total,
        CAST(overstock_units_snaive - overstock_units as int64) as improvement_units_overstock,
        CAST(understock_units_snaive - understock_units as int64) as improvement_units_understock,
         *
      FROM
        (SELECT
          --cohort,
          --bracket,
          count(*) as count,
          CAST(ROUND(SUM(week_grouped_forecast_sales - week_grouped_units)) as int64) as total_error,
          CAST(ROUND(SUM(ABS(week_grouped_forecast_sales - week_grouped_units))) as int64) as total_abs_error,
          SUM(week_snaive_units - week_grouped_units) as total_error_snaive,
          SUM(ABS(week_snaive_units - week_grouped_units)) as total_abs_error_snaive,
          SUM(week_grouped_units) as total_units,
          SUM(week_zps_units) as zps_total_units,
          CAST(ROUND(SUM(IF(week_grouped_forecast_sales > week_grouped_units, week_grouped_forecast_sales - week_grouped_units, 0))) as int64) AS overstock_units,
          SUM(IF(week_snaive_units > week_grouped_units, week_snaive_units - week_grouped_units, 0)) AS overstock_units_snaive,
          CAST(ROUND(SUM(IF(week_grouped_forecast_sales < week_grouped_units, -week_grouped_forecast_sales + week_grouped_units, 0))) as int64) AS understock_units,
          SUM(IF(week_snaive_units < week_grouped_units, -week_snaive_units + week_grouped_units, 0)) AS understock_units_snaive,
          ROUND(AVG(week_grouped_forecast_sales), 2) as Mu_forecast_sales,
          ROUND(STDDEV(week_grouped_forecast_sales), 2) as SD_forecast_sales,
          ROUND(AVG(week_grouped_units), 2) as Mu_units,
          ROUND(STDDEV(week_grouped_units), 2) as SD_units,
          ROUND(AVG(week_grouped_forecast_sales - week_grouped_units), 2) as ME,
          ROUND(AVG(week_snaive_units - week_grouped_units), 2) as ME_snaive,
          ROUND(STDDEV(week_grouped_forecast_sales - week_grouped_units), 2) as SDE,
          ROUND(STDDEV(week_snaive_units - week_grouped_units), 2) as SDE_snaive,
          ROUND(AVG(ABS(week_grouped_forecast_sales - week_grouped_units)), 2) as MAE,
          ROUND(AVG(ABS(week_snaive_units - week_grouped_units)), 2) as MAE_snaive,
          ROUND(STDDEV(ABS(week_grouped_forecast_sales - week_grouped_units)), 2) as SDAE,
          ROUND(STDDEV(ABS(week_snaive_units - week_grouped_units)), 2) as SDAE_snaive,
          ROUND(SAFE_DIVIDE(AVG(ABS(week_grouped_forecast_sales - week_grouped_units)), AVG(ABS(week_snaive_units - week_grouped_units))), 2) as MASE--,
          --ROUND(AVG(SAFE_DIVIDE(ABS(week_grouped_forecast_sales - week_grouped_units), ABS(week_snaive_units - week_grouped_units))), 2) as MASE,
          --ROUND(STDDEV(SAFE_DIVIDE(ABS(week_grouped_forecast_sales - week_grouped_units), ABS(week_snaive_units - week_grouped_units))), 2) as SDASE,
          
          -- ok_for_mase,
          --divisionId,
          --shortName
          --lookahead_weeks,
          --CAST(forecast_week_target_date as DATE) as forecast_week_target_date,
          --CAST(forecast_week_source_date as DATE) as forecast_week_source_date
          
          --DATE_ADD(CAST(ds as DATE), INTERVAL 7 DAY) as DS_END_DATE
          --forecast_month
        FROM week_grouped_forecast_and_sales
        WHERE 1=1
          --AND ds >= DATE('2020-01-01')
        -- ok_for_mase != 0
        --GROUP BY
         -- lookahead_weeks,
    --      forecast_week_target_date,
         --bracket,
        -- forecast_week_source_date
         --cohort
       ) 
    )
    --ORDER BY
      --improvementPerSale_total DESC
    --   forecast_week_target_date,
      --forecast_week_source_date
      --,lookahead_weeks
      --,bracket
    
      --cohort
    
    """

cte_1 = "SELECT * FROM `universe.galaxy.system`"
cte_2 = "SELECT * FROM `country.state.city`"
join_clause = "SELECT * FROM cte JOIN cte2 USING (planet)"

basic_str = f"""
    with cte as (
        {cte_1}
    ),
    cte2 as (
        {cte_2}
    )
    {join_clause}
    """

basic_whitespace_str = """
    with cte as (
        SELECT   *   FROM 
         `universe.galaxy.system`
    ),
    cte2 as (
                     SELECT  
                     
                     * FROM `country.state.city`
    )
    SELECT * 
    FROM cte    JOIN cte2 USING (planet)
    """