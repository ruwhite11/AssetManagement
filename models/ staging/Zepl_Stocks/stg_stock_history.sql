{{ config(MATERIALIZED='incremental') }}


with cte as
        (
          select *,
          row_number() over (partition by symbol,date order by date) num
          from {{ source('zepl_us_stocks_daily', 'stock_history') }}
        )
        select symbol, date, open, high, low, close, volume, adjclose
        from cte
        where num = 1
        order by date