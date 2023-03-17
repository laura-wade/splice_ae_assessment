# README

>1. What were the number of events, unique sessions, and unique users (i.e., full visitors) that occurred in each hour? By “hour” let's assume we mean the hour in which the event occurred.

Within a CTE I utilized BigQuery's UNNEST function to get the data at an event level and used DATE_ADD to calculate the event timestamp. In the final select I cohorted to the hour using DATE_TRUNC on the event_timestamp and performed the various count distincts necessary.

>2. Each event or hit may be associated with nested product-related fields, found in hits.product. Let's suppose we want to know the top product categories (indicated by product.v2ProductCategory) with respect to the total number of unique users who either performed a “Quickview Click”, “Product Click”, or “Promotion Click” action (as indicated by hits.eventInfo.eventAction). We also want to make sure we're analyzing true user-actions (see hits.type) and not page views.

I again wanted to get the data at the event level and then filter to only the desired user actions I also took extra precaution by filtering out PAGE, APPVIEW and EXCEPTION hits.types even though the 3 specified eventActions are only ever associated with hits.type = 'EVENT'. I checked this by doing a simple group by on the hits.type and filtering to the 3 specified eventActions. To get the top product categories I grouped by productCategory and count the distinct number of visitorIDs.

>3. Let's suppose that we ultimately want to build a model that predicts if a session that contains an “Add to Cart” action/event will be abandoned or conclude in a successful purchase. Again, we want to use hits.eventInfo.eventAction to find “Add to Cart” actions. Assuming that a session with least one transaction (indicated by totals.transactions > 0) means the session had a purchase, write a query that summarizes the number of sessions with cart additions broken out by those with and without purchases.

I expanded the data to an event level to filter to sessions that had an 'Add to Cart' event. From there I created a boolean to indicate if a purchase had been made in that session and then did a count distinct of sessions. I COALESCED the BOOLEAN in the event totals.transactions was NULL which would return NULL.

>4. Now, knowing how to determine sessions with purchases vs. sessions with abandoned carts, let's wrap this up by building a data set that we think contains useful features for a model that predicts if a session will ultimately end up with an abandoned cart or a successful purchase. In this case, feel free to explore the data and add any data you think might be meaningful. You should expand your final data set to pull from bigquery-public-data.google_analytics_sample.ga_sessions*, giving you more data to work with. Please provide a brief write up of the additional columns/features you've chosen and why you think they matter.

The data set I constructed is at the session level and is filtered to only sessions that had an 'Add to Cart' action. 
Features:
- visit_start_timestamp/visit_start_day_of_week/visit_start_hour: 
  -  There are certain times that people are more intent on making a purchase. It's possible weekends have higher intent or late nights have higher abandon rates.
- device_category: 
  - A specific device type could have higher abandon rates due to the user experience on mobile vs desktop.
- traffic_source_medium: 
  - Referred traffic could have lower intent than traffic that came directly to the website.
- is_first_time_visitor: 
  - First-time vs. Repeat visitors could have higher abandon rates due to lack of familiarity with the site/brand etc.
- num_product_clicks/num_promotion_clicks/num_add_to_cart_actions/num_remove_from_cart_actions:
  - How much did the visitor/user actively interact with the site? Do product clicks show higher intent over promotion clicks? Do more cart management actions indicate higher intent?
- cumulative_transaction_revenue:
  - The total transaction revenue for the visitor over all prior sessions. If the visitor has previously spent a certain amount of money on the site are they more likely to make a purchase in that session.
- visitor_previously_purchased: 
  - Now this one I could have just used cumulative_transaction_revenue > 0, but I wanted to use FIRST_VALUE just for the sake of displaying my SQL skills. This goes a step further from is_first_time_visitor and would help us look at the relationship between abandoning cart when a visitor previously purchased during another session.
- added_top_5_abandoned_product_to_cart:
  - Looking at the items the user added to cart, were any of these items a top 5 abandoned item in the last 30 days. For each day in the dataset I calculated the top 5 products by number of users who abandoned that item in their cart in the previous 30 days. This feature would help us determine if certain products are frequently abandoned by users.
