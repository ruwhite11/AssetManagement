{{ config(MATERIALIZED='table') }}

select symbol, exchange, companyname, industry, website, description, ceo, sector, beta, mktcap::number mktcap
        from {{ source('zepl_us_stocks_daily', 'company_profile') }}