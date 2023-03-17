/*
1. What were the number of events, unique sessions, and unique users (i.e., full visitors) that occurred in each hour? 
By “hour” let's assume we mean the hour in which the event occurred.
*/

WITH adjust_timestamps AS (
  SELECT 
    DATE_ADD(TIMESTAMP_SECONDS(visitStartTime), INTERVAL hits.time MILLISECOND) AS event_timestamp,
    fullVisitorId,
    visitId,
    fullVisitorID || '_' || visitID AS session_id,
    fullVisitorID || '_' || visitID || '_' || hits.hitNumber AS event_id
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
    UNNEST(hits) AS hits
)
SELECT DATE_TRUNC(event_timestamp,HOUR) AS event_date_hour,
  COUNT(DISTINCT fullVisitorId) AS unique_users,
  COUNT(DISTINCT session_id) AS unique_sessions,
  COUNT(DISTINCT event_id) AS total_events
FROM adjust_timestamps
GROUP BY 1
ORDER BY 1 DESC;

/*
2. Each event or hit may be associated with nested product-related fields, found in hits.product. 
Let's suppose we want to know the top product categories (indicated by product.v2ProductCategory) with respect to the total number of unique users who either performed a “Quickview Click”, “Product Click”, or “Promotion Click” action (as indicated by hits.eventInfo.eventAction). 
We also want to make sure we're analyzing true user-actions (see hits.type) and not page views.
*/

SELECT 
  products.v2ProductCategory AS product_category,
  COUNT(DISTINCT fullVisitorId) AS unique_users_clicked
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
  UNNEST(hits) AS hits,
  UNNEST(hits.product) AS products
WHERE hits.eventInfo.eventAction IN ('Quickview Click', 'Product Click', 'Promotion Click')
  AND hits.type IN ("TRANSACTION", "ITEM", "EVENT", "SOCIAL") --assuming PAGE, APPVIEW are view events and EXCEPTION is an error and not a true user event
GROUP BY 1
ORDER BY 2 DESC;


/*
3. Let's suppose that we ultimately want to build a model that predicts if a session that contains an “Add to Cart” action/event will be abandoned 
or conclude in a successful purchase. Again, we want to use hits.eventInfo.eventAction to find “Add to Cart” actions. 
Assuming that a session with least one transaction (indicated by totals.transactions > 0) means the session 
had a purchase, write a query that summarizes the number of sessions with cart additions broken out by those with and without purchases.
*/
WITH add_to_cart_sessions AS (
  SELECT 
    fullVisitorId,
    visitId,
    COALESCE(totals.transactions > 0, FALSE) AS session_had_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
    UNNEST(hits) AS hits
  WHERE hits.eventInfo.eventAction = 'Add to Cart'
  GROUP BY 1, 2, 3
)
SELECT 
  session_had_purchase,
  COUNT(DISTINCT fullvisitorId || '_' || visitId) AS unique_add_to_cart_sessions
FROM add_to_cart_sessions
GROUP BY 1;

/*
4. Now, knowing how to determine sessions with purchases vs. sessions with abandoned carts, let's wrap this up by building a data set that we think contains useful features for a model that predicts if a session will ultimately end up with an abandoned cart or a successful purchase. In this case, feel free to explore the data and add any data you think might be meaningful. You should expand your final data set to pull from bigquery-public-data.google_analytics_sample.ga_sessions*, giving you more data to work with. Please provide a brief write up of the additional columns/features you've chosen and why you think they matter.
*/

--For each day get the top 5 abandoned products by the 30 day rolling average of users who abandoned the product
--We only need cart events for sessions that did not have a purchase
CREATE OR REPLACE TABLE `test.top_5_abandoned_products_by_day` AS
WITH add_to_cart AS (
  SELECT 
    TIMESTAMP_SECONDS(visitStartTime) AS visit_start_timestamp,
    fullvisitorId,
    visitId,
    products.productSKU AS productSKU,
    products.productQuantity AS productQuantity_added
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits,
    UNNEST(product) AS products
  WHERE hits.eventInfo.eventAction = 'Add to Cart'
    AND totals.transactions IS NULL
  GROUP BY 1,2,3,4,5
)
, remove_from_cart AS (
  SELECT 
    TIMESTAMP_SECONDS(visitStartTime) AS visit_start_timestamp,
    fullvisitorId,
    visitId,
    products.productSKU AS productSKU,
    products.productQuantity AS productQuantity_removed
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits,
    UNNEST(product) AS products
  WHERE hits.eventInfo.eventAction = 'Remove from Cart'
    AND totals.transactions IS NULL
  GROUP BY 1,2,3,4,5
)
, users_abandoned_by_date AS (
  SELECT 
    DATE(atc.visit_start_timestamp) AS visit_date,
    atc.productSKU,
    COUNT(DISTINCT CASE WHEN atc.productQuantity_added - COALESCE(rfc.productQuantity_removed, 0) > 0 THEN atc.fullvisitorID END) AS users_abandoned_product,
  FROM add_to_cart atc
  LEFT JOIN remove_from_cart rfc
    ON atc.fullVisitorId = rfc.fullVisitorId
    AND atc.visitId = rfc.visitId
    AND atc.productSKU = rfc.productSKU
  WHERE rfc.visitId IS NULL
  GROUP BY 1, 2
)
, rolling_30_day AS (
  SELECT visit_date,
    productSKU,
    AVG(users_abandoned_product) OVER(PARTITION BY productSKU ORDER BY visit_date ASC ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as rolling_30_day_abandon_average
  FROM users_abandoned_by_date
)
, rank_products AS (
SELECT visit_date,
  productSKU,
  ROW_NUMBER() OVER(PARTITION BY visit_date ORDER BY rolling_30_day_abandon_average DESC) AS daily_abandoned_product_rank
FROM rolling_30_day
)
SELECT *
FROM rank_products
WHERE daily_abandoned_product_rank <= 5;

--Identify if a session add any of the top 5 abandoned items to their cart on that day
CREATE OR REPLACE TABLE `test.visitor_session_has_top_5_abandoned_product` AS
WITH add_to_cart AS (
  SELECT 
    TIMESTAMP_SECONDS(visitStartTime) AS visit_start_timestamp,
    DATE(TIMESTAMP_SECONDS(visitStartTime)) AS visit_start_date,
    fullvisitorId,
    visitId,
    products.productSKU as productSKU
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits,
    UNNEST(product) AS products
  WHERE hits.eventInfo.eventAction = 'Add to Cart'
  GROUP BY 1,2,3,4,5
)
SELECT atc.fullVisitorId,
  atc.visitID,
  LOGICAL_OR(top.productSKU IS NOT NULL) AS added_top_5_abandoned_product_to_cart,
FROM add_to_cart atc
LEFT JOIN test.top_5_abandoned_products_by_day top
  ON atc.productSKU = top.productSKU
  AND atc.visit_start_date = top.visit_date
GROUP BY 1, 2;

--Aggregate the final prediction dataset
CREATE OR REPLACE TABLE `test.abandoned_cart_prediction_dataset` AS
WITH add_to_cart_sessions AS (
  SELECT 
    fullVisitorId,
    visitId,
    visitStartTime,
    device.deviceCategory AS device_category,
    trafficSource.medium AS traffic_source_medium,
    visitNumber = 1 AS is_first_time_visitor,
    totals.transactions AS total_transactions,
    totals.totalTransactionRevenue,
    ROUND(totals.totalTransactionRevenue/1000000, 2) AS total_transaction_revenue,
    SUM(CASE WHEN hits.eventInfo.eventAction = 'Add to Cart' THEN 1 END) AS num_add_to_cart_actions,
    SUM(CASE WHEN hits.eventInfo.eventAction = 'Remove from Cart' THEN 1 END) AS num_remove_from_cart_actions,
    SUM(CASE WHEN hits.eventInfo.eventAction = 'Product Click' THEN 1 END) AS num_product_clicks,
    SUM(CASE WHEN hits.eventInfo.eventAction = 'Promotion Click' THEN 1 END) AS num_promotion_clicks,
    --calculating visitor level total transaction revenue prior to the current session
    ROUND(SUM(COALESCE(totals.totalTransactionRevenue,0)/1000000) OVER(PARTITION BY fullVisitorId ORDER BY TIMESTAMP_SECONDS(visitStartTime) ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 2) AS cumulative_transaction_revenue,
    --identifying each visitors first visitID that had a purchase to determine if visitor previous made a purchase.
    FIRST_VALUE(CASE WHEN COALESCE(totals.transactions > 0, FALSE) THEN visitId END) OVER(PARTITION BY fullVisitorId ORDER BY TIMESTAMP_SECONDS(visitStartTime) ASC) AS first_visitID_with_purchase,
    --deduping sessions, noticed some with different visitStarTime so taking the first one only
    ROW_NUMBER() OVER(PARTITION BY fullVisitorId, visitId ORDER BY TIMESTAMP_SECONDS(visitStartTime) ASC) = 1 AS is_first_session 
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
  HAVING SUM(CASE WHEN hits.eventInfo.eventAction = 'Add to Cart' THEN 1 END) > 0 --only pull sessions with an add to cart event
)
SELECT 
  acsd.fullVisitorId,
  acsd.visitId,
  TIMESTAMP_SECONDS(visitStartTime) AS visit_start_timestamp,
  EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(visitStartTime)) AS visit_start_day_of_week,
  EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) AS visit_start_hour,
  device_category,
  traffic_source_medium,
  is_first_time_visitor,
  num_add_to_cart_actions,
  num_remove_from_cart_actions,
  num_product_clicks,
  num_promotion_clicks,
  cumulative_transaction_revenue,
  first_visitID_with_purchase IS NOT NULL AND first_visitID_with_purchase <> acsd.visitID AS visitor_previously_purchased,
  ap.added_top_5_abandoned_product_to_cart,
  COALESCE(total_transactions > 0, FALSE) AS session_had_purchase
FROM add_to_cart_sessions acsd
LEFT JOIN test.visitor_session_has_top_5_abandoned_product ap
  ON acsd.fullVisitorId = ap.fullVisitorId
  AND acsd.visitId = ap.visitID
WHERE is_first_session
;
