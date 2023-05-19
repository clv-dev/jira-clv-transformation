WITH dim_date__generate AS (
  SELECT
    *
  FROM UNNEST(GENERATE_DATE_ARRAY('2010-01-01','2030-12-31',interval 1 day)) AS date
)

, dim_date__extract_components AS (
  SELECT
    CAST(date AS date) AS date
    , FORMAT_DATE('%A',date) AS day_of_week
    , FORMAT_DATE('%a',date) AS day_of_week_short
    , EXTRACT(ISOWEEK FROM date) AS week_number
    , FORMAT_DATE('%G%V', date) AS year_week_number
    , FORMAT_DATE('%G/W%V',date) AS year_week
    , FORMAT_DATE('%Y%m', date) AS year_month_number
    , FORMAT_DATE('%Y/M%m',date) AS year_month
    , EXTRACT(MONTH FROM date) AS month
    , FORMAT_DATE('%B',date) AS month_name
    , FORMAT_DATE('%Y%Q', date) AS year_quarter_number
    , FORMAT_DATE('%Y/Q%Q',date) AS year_quarter
    , EXTRACT(QUARTER FROM date) AS quarter
    , EXTRACT(YEAR FROM date) AS year
  FROM dim_date__generate
)

SELECT
  dim_date.date
  , dim_date.day_of_week
  , dim_date.day_of_week_short
  , CASE
      WHEN dim_date.day_of_week_short IN ('Sat', 'Sun') THEN 'Weekend'
      WHEN dim_date.day_of_week_short IN ('Mon', 'Tue', 'Wed', 'Thu', 'Fri') THEN 'Weekday'
      ELSE 'Invalid' END
  AS is_weekday_or_weekend
  , dim_date.week_number
  , dim_date.year_week_number
  , dim_date.year_week
  , dim_date.month_name
  , dim_date.month
  , dim_date.year_month_number
  , dim_date.year_month
  , dim_date.quarter
  , dim_date.year_quarter_number
  , dim_date.year_quarter
  , dim_date.year
  , CASE
      WHEN dim_holiday.holiday_date IS NOT NULL THEN 'Holiday'
      WHEN dim_holiday.holiday_date IS NULL THEN 'Not Holiday'
      ELSE 'Invalid' END
    AS is_holiday
  , IFNULL(dim_holiday.holiday_name, 'Undefined') AS holiday_name
  , CASE
      WHEN date BETWEEN '2023-02-15' AND '2023-02-28' THEN 'PI8 Iteration 1'
      WHEN date BETWEEN '2023-03-01' AND '2023-03-14' THEN 'PI8 Iteration 2'
      WHEN date BETWEEN '2023-03-15' AND '2023-03-28' THEN 'PI8 Iteration 3'
      WHEN date BETWEEN '2023-03-29' AND '2023-04-11' THEN 'PI8 Iteration 4'
      WHEN date BETWEEN '2023-04-12' AND '2023-05-03' THEN 'PI8 Iteration 5'
      WHEN date BETWEEN '2023-05-08' AND '2023-05-14' THEN 'PI9 Planning Week'
      WHEN date BETWEEN '2023-05-17' AND '2023-05-30' THEN 'PI9 Iteration 1'
      WHEN date BETWEEN '2023-05-31' AND '2023-06-13' THEN 'PI9 Iteration 2'
      WHEN date BETWEEN '2023-06-14' AND '2023-06-27' THEN 'PI9 Iteration 3'
      WHEN date BETWEEN '2023-06-28' AND '2023-07-11' THEN 'PI9 Iteration 4'
      WHEN date BETWEEN '2023-07-12' AND '2023-07-25' THEN 'PI9 Iteration 5'
      ELSE 'Undefined' END
  AS dde_iteration
FROM dim_date__extract_components AS dim_date
LEFT JOIN `looker-team-management-386803.jira_clv_staging.staging__holiday` AS dim_holiday
  ON dim_date.date = dim_holiday.holiday_date