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
ORDER BY 1 DESC

/*
2. Each event or hit may be associated with nested product-related fields, found in hits.product. 
Let's suppose we want to know the top product categories (indicated by product.v2ProductCategory) with respect to the total number of unique users who either performed a “Quickview Click”, “Product Click”, or “Promotion Click” action (as indicated by hits.eventInfo.eventAction). 
We also want to make sure we're analyzing true user-actions (see hits.type) and not page views.
*/

SELECT 
  products.v2ProductCategory as product_category,
  COUNT(DISTINCT fullVisitorId) AS unique_users_clicked
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
  UNNEST(hits) AS hits,
  UNNEST(hits.product) as products
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
GROUP BY 1

/*
4. Now, knowing how to determine sessions with purchases vs. sessions with abandoned carts, let's wrap this up by building a data set that we think contains useful features for a model that predicts if a session will ultimately end up with an abandoned cart or a successful purchase. In this case, feel free to explore the data and add any data you think might be meaningful. You should expand your final data set to pull from bigquery-public-data.google_analytics_sample.ga_sessions*, giving you more data to work with. Please provide a brief write up of the additional columns/features you've chosen and why you think they matter.
*/

WITH add_to_cart_sessions AS (
  SELECT 
    fullVisitorId,
    visitId,
    visitStartTime,
    device.deviceCategory as device_category,
    trafficSource.medium as traffic_source_medium,
    visitNumber = 1 AS is_first_time_visitor,
    totals.transactions AS total_transactions,
    totals.totalTransactionRevenue,
    ROUND(totals.totalTransactionRevenue/1000000, 2) as total_transaction_revenue,
    SUM(CASE WHEN hits.eventInfo.eventAction = 'Add to Cart' THEN 1 END) AS num_add_to_cart_actions,
    SUM(CASE WHEN hits.eventInfo.eventAction = 'Remove from Cart' THEN 1 END) AS num_remove_from_cart_actions,
    SUM(CASE WHEN hits.eventInfo.eventAction = 'Add to Cart' THEN 1 END) - COALESCE(SUM(CASE WHEN hits.eventInfo.eventAction = 'Remove from Cart' THEN 1 END),0) AS items_in_cart,
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
  fullVisitorId,
  visitId,
  visitStartTime,
  device_category,
  traffic_source_medium,
  is_first_time_visitor,
  total_transactions,
  total_transaction_revenue,
  num_add_to_cart_actions,
  num_remove_from_cart_actions,
  items_in_cart,
  num_product_clicks,
  num_promotion_clicks,
  cumulative_transaction_revenue,
  COALESCE(total_transactions > 0, FALSE) AS session_had_purchase,
  first_visitID_with_purchase is not null and first_visitID_with_purchase <> visitID AS visitor_previously_purchased
from add_to_cart_sessions acsd
WHERE is_first_session
;
