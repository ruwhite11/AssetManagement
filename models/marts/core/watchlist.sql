{{
  config(
    materialized = 'table',
    )
}}
select *, 'charles'::varchar(50) Trader
            from {{ ref('stg_company_profile') }}
            where symbol in ('AMZN','CAT','COF','GE','GOOG','MCK','MSFT','NFLX','SBUX','TSLA','VOO','XOM')
                union all
            select * from {{ ref('temp_watchlist') }}
            order by trader, symbol, exchange