# Temporal Rating Trend Analysis

Leverage **Apache Pig** and **HDFS** to analyze movie ratings over time by extracting the **year** from each rating’s timestamp. This allows you to compute and visualize average ratings per year, providing insights for various strategic use cases.

---

## Use Cases

### 1. Movie Production Insights
- **Problem:**  
  A movie studio wants to assess the quality and reception of its movies over the years.
- **Solution:**  
  Analyze average ratings of movies per year to evaluate trends in viewer satisfaction.  
  - Adjust production strategies (e.g., genres, directors, actors) based on these trends.

### 2. Marketing Campaign Design
- **Problem:**  
  A marketing team wants to promote movies from years with higher average ratings to emphasize quality.
- **Solution:**  
  Highlight movies from highly-rated years in promotional campaigns.  
  - Use trends to target audiences nostalgic for older, highly-rated movies.

### 3. Academic and Cultural Studies
- **Problem:**  
  Researchers or institutions want to analyze how cultural shifts or global events affect viewer preferences over time.
- **Solution:**  
  Use temporal trends to correlate rating patterns with historical or cultural events.

---

## Pig Script for Temporal Analysis

Below is an example Pig script (`temporal_rating_trend.pig`) that illustrates how to:
1. Load the ratings data  
2. Extract the **year** from the timestamp  
3. Group and compute the **average rating** per year  
4. Sort the results by year  
5. Store the final output in HDFS

```pig
-- Load the ratings data with correct schema
ratings = LOAD '/final_project/ratings.csv'
           USING PigStorage(',')
           AS (userId:int, movieId:int, rating:float, tstamp:chararray);

-- Extract year directly from tstamp
ratings_by_year = FOREACH ratings GENERATE
                   SUBSTRING(tstamp, LAST_INDEX_OF(tstamp, '/') + 1, 4) AS year,
                   rating;

-- Group by year
grouped_ratings = GROUP ratings_by_year BY year;

-- Calculate average rating per year
avg_rating_per_year = FOREACH grouped_ratings GENERATE
                      group AS year,
                      ROUND_TO(AVG(ratings_by_year.rating), 2) AS avg_rating;

-- Sort results by year
sorted_results = ORDER avg_rating_per_year BY year;

-- Store the results
STORE sorted_results INTO '/user/hdoop/avg_rating_per_year' USING PigStorage(',');
```

# Temporal Rating Trend Analysis

Leverage **Apache Pig** and **HDFS** to analyze movie ratings over time by extracting the **year** from each rating’s timestamp. This allows you to compute and visualize average ratings per year, providing insights for various strategic use cases.

---

## Use Cases

### 1. Movie Production Insights
- **Problem:**  
  A movie studio wants to assess the quality and reception of its movies over the years.
- **Solution:**  
  Analyze average ratings of movies per year to evaluate trends in viewer satisfaction.  
  - Adjust production strategies (e.g., genres, directors, actors) based on these trends.

### 2. Marketing Campaign Design
- **Problem:**  
  A marketing team wants to promote movies from years with higher average ratings to emphasize quality.
- **Solution:**  
  Highlight movies from highly-rated years in promotional campaigns.  
  - Use trends to target audiences nostalgic for older, highly-rated movies.

### 3. Academic and Cultural Studies
- **Problem:**  
  Researchers or institutions want to analyze how cultural shifts or global events affect viewer preferences over time.
- **Solution:**  
  Use temporal trends to correlate rating patterns with historical or cultural events.

---

## Pig Script for Temporal Analysis

Below is an example Pig script (`temporal_rating_trend.pig`) that illustrates how to:
1. Load the ratings data  
2. Extract the **year** from the timestamp  
3. Group and compute the **average rating** per year  
4. Sort the results by year  
5. Store the final output in HDFS

```pig
-- Load the ratings data with correct schema
ratings = LOAD '/final_project/ratings.csv'
           USING PigStorage(',')
           AS (userId:int, movieId:int, rating:float, tstamp:chararray);

-- Extract year directly from tstamp
ratings_by_year = FOREACH ratings GENERATE
                   SUBSTRING(tstamp, LAST_INDEX_OF(tstamp, '/') + 1, 4) AS year,
                   rating;

-- Group by year
grouped_ratings = GROUP ratings_by_year BY year;

-- Calculate average rating per year
avg_rating_per_year = FOREACH grouped_ratings GENERATE
                      group AS year,
                      ROUND_TO(AVG(ratings_by_year.rating), 2) AS avg_rating;

-- Sort results by year
sorted_results = ORDER avg_rating_per_year BY year;

-- Store the results
STORE sorted_results INTO '/user/hdoop/avg_rating_per_year' USING PigStorage(',');
