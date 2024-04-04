# bq-shared-cache
Bigquery helper tool in python 3. Allows caching the results of queries and subqueries in tables implicitly to reduce necessary processing.

Although personal caching is a feature in bigquery, there are no mechanisms to cache queries between users. 

This library will store the results of queries in tables temporarily and index them using the source code of the queries used to produce them as an evaluation of similarity. 
