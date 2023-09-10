-- models/incremental_model.sql

-- Use the 'incremental' config to specify the strategy
{{ config(
    materialized='incremental'
) }}

WITH transformed_data AS (
  SELECT
    transactionamount AS amt,
    age,
    12 AS trans_month,
    2020 AS trans_year,
    homelatitude - transactionlatitude AS latitudinal_distance,
    homelongitude - transactionlongitude AS longitudinal_distance,
    cardnumber,
    timestamp AS timestamp,
    transactionlatitude,
    transactionlongitude,
  FROM
    `demo.raw_transaction`
)

-- Select the transformed data
SELECT
  amt,
  age,
  trans_month,
  trans_year,
  latitudinal_distance,
  longitudinal_distance,
  cardnumber,
  timestamp,
  transactionlatitude,
  transactionlongitude,
FROM transformed_data
WHERE cardnumber NOT IN (SELECT cardnumber FROM `demo.formatted_transaction`)


{% if is_incremental() %}
AND updated_at > (SELECT max(updated_at) FROM `demo.formatted_transaction`)
{% endif %}
