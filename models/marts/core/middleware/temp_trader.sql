{{
  config(
    materialized = 'table' ,schema='middleware',
    )
}}
with cte as (

    select distinct *
      from
      (
          select
              upper(randstr(3, random()))::varchar(50) trader,
              upper(randstr(2, random()))::varchar(50) PM,
              uniform(4000, 8000, random())::number buying_power
          from table (generator(rowcount => 60000))
      ) c
)

select distinct *
from cte

where rlike(trader,'[A-Z][A-Z][A-Z]') = 'TRUE' and rlike(PM,'[A-Z][A-Z]') = 'TRUE'



