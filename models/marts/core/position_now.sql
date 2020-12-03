{{
  config(
    materialized = 'incremental',
    )
}}
select p.*, l.close, l.date,
            num_share_now * close as market_value,
            (num_share_now * close) + cash_now as PnL
        from {{ ref('share_now') }} p
        left outer join {{ ref('stg_stock_latest') }} l on p.symbol = l.symbol