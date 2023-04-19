WITH dim_date__generate AS (
  SELECT
    *
  FROM GENERATE_SERIES(date'2010-01-01', date'2030-12-31', '1 day') AS date
)

, dim_date__recast AS (
  SELECT
    date::date
  FROM dim_date__generate
)

, dim_date__extract_components AS (
  SELECT
    date
    , TO_CHAR(date,'Day') AS day_of_week
    , TO_CHAR(date,'Dy') AS day_of_week_short
    , EXTRACT(WEEK FROM date) AS week
    , REPLACE(TO_CHAR(date,'YYYY/IW'), '/', '/W') AS year_week
    , EXTRACT(MONTH FROM date) AS month
    , REPLACE(TO_CHAR(date,'YYYY/MM'), '/', '/M') AS year_month
    , TO_CHAR(date, 'Month') AS month_name
    , TO_CHAR(date, 'Mon') as month_name_short
    , EXTRACT(QUARTER FROM date) AS quarter
    , REPLACE(TO_CHAR(date, 'YYYY/QQ'), '/', '/Q') AS year_quarter
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
  , dim_date.week
  , dim_date.year_week
  , dim_date.month_name
  , dim_date.month
  , dim_date.year_month
  , dim_date.quarter
  , dim_date.year_quarter
  , dim_date.year
FROM dim_date__extract_components AS dim_date
