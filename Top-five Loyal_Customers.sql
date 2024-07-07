WITH
  loyality_points_data AS (
  SELECT
    lp.*,
    CAST(REGEXP_EXTRACT(Loyalty_Points,r'\d+\.\d+|\d+') AS NUMERIC) AS L_point,
    REGEXP_EXTRACT(Email_Address, r'^([^\.]+)') AS first_name,
    SUBSTR(REGEXP_EXTRACT(Email_Address, r'\.([^@]+)'), 1, 1) AS last_name,
    `Store ID` AS store_id
  FROM
    `infinite-pad-424305-d7.github_dataset.loyality_point` lp ),
  customer_data AS (
  SELECT
    *
  FROM
    `infinite-pad-424305-d7.github_dataset.customer_details`
  WHERE
    Postcode IS NOT NULL ),
  store_data AS (
  SELECT
    sd.*,
    `Store ID` AS store_id
  FROM
    `infinite-pad-424305-d7.github_dataset.store_data` sd ),
  combined_data AS (
  SELECT
    c City,
    Store,
    Email_Address,
    lpd.first_Name,
    lpd.last_Name,
    L_point loyality_points,
    DateTime_Out Date,
    Postcode,
    Address --lpd.`Store ID`
  FROM
    loyality_points_data lpd
  LEFT JOIN
    customer_data cd
  ON
    lpd.first_name=cd.first_Name
    AND (UPPER(lpd.last_name) = UPPER(LEFT(cd.last_Name, 1)))
  LEFT JOIN
    store_data sd
  ON
    lpd.store_id=sd.store_id  ),
    post_code_notnull AS (
    SELECT
      *
    FROM
      combined_data cd
    WHERE
      postcode IS NOT NULL ),
    rank_data AS (
    SELECT
      pcn.*,
      DENSE_RANK() OVER (PARTITION BY store ORDER BY loyality_points DESC) AS rank
    FROM
      post_code_notnull pcn )
  SELECT
    *
  FROM
    rank_data
  WHERE
    rank <=5
