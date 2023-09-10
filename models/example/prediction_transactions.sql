-- models/incremental_model.sql
#standardSQL
-- Use the 'incremental' config to specify the strategy
{{ config(
    materialized='incremental'
) }}

WITH transformed_data AS (
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
    transactionlongitude
  FROM
    `demo.formatted_transaction`
),

Predictions AS (
  SELECT
    *,
    IF(predicted_label = is_fraud, 1.0, 0.0) AS is_correct
  FROM
    ML.PREDICT(MODEL `demo.rfc_final`, (
        amt,
        age,
        trans_month,
        trans_year,
        latitudinal_distance,
        longitudinal_distance
    ))
)

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
  is_correct AS is_fraud
FROM transformed_data
WHERE cardnumber NOT IN (SELECT cardnumber FROM `demo.prediction_transaction`)

{% if is_incremental() %}
AND updated_at > (SELECT max(updated_at) FROM `demo.prediction_transaction`)
{% endif %}
