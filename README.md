# Discounted-cumulative-gain
Discounted cumulative gain (DCG) is a measure of ranking quality. In information retrieval, it is often used to measure effectiveness of web search engine algorithms or related applications. Using a graded relevance scale of documents in a search-engine result set, DCG measures the usefulness, or gain, of a document based on its position in the result list. 
The gain is accumulated from the top of the result list to the bottom, with the gain of each result discounted at lower ranks.

Here I present a pyspark implementation created for a practical test, due to this the code is data format specific but given adjusting the data reading part the DCG logic still holds.

## Data format

The main two data sources used are query and clicks data.

- The clicks dataframe contains the interactions with store objects given a query. Below a sample of clicks dataframe:
```
+--------------------------+------------------------+---+------+-----------+------+
|query                     |ivm                     |imp|clicks|add_to_cart|orders|
+--------------------------+------------------------+---+------+-----------+------+
|capture dry carpet cleaner|sdcs-scsd-vsd12-3211c   |1  |0     |0          |0     |
|capture dry carpet cleaner|123df-2e3-325f-43f3eew-4|1  |0     |0          |0     |
|capture dry carpet cleaner|2241412df-3dw44f-wy65   |1  |0     |0          |0     |
|capture dry carpet cleaner|65uhw-ejw46wgex-x56     |1  |0     |0          |0     |
|capture dry carpet cleaner|14-3fcy4vy4n7-n86       |1  |1     |0          |0     |
+--------------------------+------------------------+---+------+-----------+------+
```
- The query dataframe contains the results of the items relevance scoring given a query. Below a sample of query dataframe and its schema:
```
+------------------------------------------------------------------------------------------------------------------------------+--------------------------+
|items                                                                                                                         |query                     |
+------------------------------------------------------------------------------------------------------------------------------+--------------------------+
|[{IVM_s, 2.0, exp_nrt, a1-a1-a1-a1-a1-a1-a1-a1}, {IVM_s, 1.134082982524361, exp_nrt, b2-b2-b2-b2-b2-b2-b2-b},                 |                          |
| {IVM_s, 1.082411640378537, exp_nrt, c3-c3-c3-c3-c3-c3-c3-c}, {IVM_s, 1.0009327106444963, exp_nrt, d4-d4-d4-d4-d4-d4-d4-d4}]  |capture dry carpet cleaner|
+------------------------------------------------------------------------------------------------------------------------------+--------------------------+

|-- items: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- field: string (nullable = true)
 |    |    |-- score: double (nullable = true)
 |    |    |-- source: string (nullable = true)
 |    |    |-- value: string (nullable = true)
 |-- query: string (nullable = true)
```

## Objectives
The main goal of this coding exercise is to combine eCommerce product search results data with click-stream logs collected from live search traffic. For each search query sent to search engine (catalog), a list of products is returned as results. Products are visualized on the screen and users interacted with the list by a number of behavioral signals such as impressions, clicks, adds to cart, and orders which are tracked in the click-stream data by counting them for each the product id, labeled as ivm.

Write a pySpark script that merges query and clickstream data to return a ranking table like the following:
```
+--------------------------+-----------------------+---------+---+------+-----------+------+
|query                     |ivm                    |score    |imp|clicks|add_to_cart|orders|
+--------------------------+-----------------------+---------+---+------+-----------+------+
|capture dry carpet cleaner|836143-68128-3000092674|2.0      |55 |52    |16         |10    |
|capture dry carpet cleaner|15673-68128-500467-705 |1.134083 |0  |0     |0          |0     |
|capture dry carpet cleaner|18055-68128-3000005244 |1.0824116|10 |9     |0          |0     |
|capture dry carpet cleaner|2462396-99630-DX5G-BNDL|1.0009327|0  |0     |0          |0     |
|capture dry carpet cleaner|4883237-82757-FFP4263  |1.0000457|1  |1     |0          |0     |
|capture dry carpet cleaner|440597-1684-8352       |1.0000457|0  |0     |0          |0     |
|capture dry carpet cleaner|617329-69384-19X6      |1.0000457|1  |1     |0          |0     |
|capture dry carpet cleaner|9704-20097-9030411     |1.0000365|0  |0     |0          |0     |
|capture dry carpet cleaner|2597282-1684-2035M     |1.0000272|0  |0     |0          |0     |
|capture dry carpet cleaner|1296053-69384-BGDC-21  |1.0000011|0  |0     |0          |0     |
|capture dry carpet cleaner|2597281-1684-2035A     |1.0      |0  |0     |0          |0     |
+--------------------------+-----------------------+---------+---+------+-----------+------+
```
It is important that:
- The code is object-oriented with well designed classes and clean code 
- The solution scales to millions of queries and clickstream records
- The code has clean command line input / output parameters

Bonus:
- Calculate the average nDCG@10 for the click column