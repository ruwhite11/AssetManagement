### Asset Management System with Snowflake and DBT

#### Description

This is a Sample Asset Management project forked from Allen Wong's Snowflake Asset Management Demo which can be found at the following link https://github.com/allen-wong-tech/asset-management. This project goes one step further and adds dbt into the mix to manage the system.

#### Key Features shown in this Project

1. Snowflake - a leader in the data warehouse space. Snowflake gives great performance by separating storage and compute which makes it massively scalable.
2. Snowflake Marketplace - a key source for this project is the Zepl Stock History database which is synced automatically to my snowflake datawarehouse. This data will always be up to date without any ETL needed.
3. DBT - used to manage all of the transformations needed to build this data warehouse. Dbt makes our project easy to test along with following software engineering best practices. 
4. DBT Cloud (free developer version) - refreshes our data warehouse once a day based on our github repo main branch.

