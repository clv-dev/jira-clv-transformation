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
  date
  , day_of_week
  , day_of_week_short
  , CASE
      WHEN day_of_week_short IN ('Sat', 'Sun') THEN 'Weekend'
      WHEN day_of_week_short IN ('Mon', 'Tue', 'Wed', 'Thu', 'Fri') THEN 'Weekday'
      ELSE 'Invalid' END
    AS is_weekday_or_weekend
  , week
  , year_week
  , month_name
  , month
  , year_month
  , quarter
  , year_quarter
  , year
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
FROM dim_date__extract_components