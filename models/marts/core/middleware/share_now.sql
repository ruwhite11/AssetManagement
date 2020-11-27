{{
  config(
    materialized = 'view',
    )
}}
with cte as
          (
            select
                symbol, exchange, trader, pm,
                last_value(num_shares_cumulative) over (partition by symbol, exchange, trader order by date) as num_share_now,
                last_value(cash_cumulative) over (partition by symbol, exchange, trader order by date) as cash_now,
                case when last_value(date) over (partition by symbol, exchange, trader order by date) = date then 1 else 0 end is_current
            from {{ ref('position') }}
          )
          select symbol, exchange, trader, pm, num_share_now, cash_now
          from cte
          where is_current = 1