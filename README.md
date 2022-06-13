# Discounted-cumulative-gain
Discounted cumulative gain (DCG) is a measure of ranking quality. In information retrieval, it is often used to measure effectiveness of web search engine algorithms or related applications. Using a graded relevance scale of documents in a search-engine result set, DCG measures the usefulness, or gain, of a document based on its position in the result list. 
The gain is accumulated from the top of the result list to the bottom, with the gain of each result discounted at lower ranks.

Here I present a pyspark implementation created for a practical test, due to this the code is data format specific but given adjusting the data reading part the DCG logic still holds.
