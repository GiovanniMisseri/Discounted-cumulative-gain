import findspark
findspark.init()

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as f
import argparse

class query_stats_on_clicks:

  def __init__(self, path_query, path_clicks):
    self.path_query = path_query
    self.path_clicks = path_clicks

  def get_spark(self):
    spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()
    return spark

  def query(self):
    spark = self.get_spark()

    query = spark.read.json(self.path_query)
    explod_query = (query
                    .withColumn('exploded_items', f.explode('items'))
                    .select('exploded_items.*', 'query')
                    .selectExpr('query', 'value as ivm', 'score')
                    )
    return explod_query

  def clicks(self):
    spark = self.get_spark()
    
    clicks = spark.read.parquet(self.path_clicks) 
    return clicks

  def stats_query_clicks(self):
    clicks = self.clicks()
    explod_query = self.query()

    return explod_query.join(clicks, ['query','ivm'], 'left').fillna(0).orderBy('query',-f.col('score'))

  def nDCG(self, rows_ndcg):
    stats_query_clicks = self.stats_query_clicks()

    nDCG = (stats_query_clicks 
            .withColumn('row_number', f.row_number().over(Window.partitionBy('query').orderBy(-f.col('score'))))
            .filter(f.col('row_number')<=rows_ndcg)
            .withColumn('ideal_proportional_relevance', 
                                f.col('clicks')/f.log2(f.row_number().over(Window.partitionBy('query').orderBy(-f.col('clicks'))) +1))
            .withColumn('proportional_relevance', 
                                f.col('clicks')/f.log2(f.row_number().over(Window.partitionBy('query').orderBy(-f.col('score'))) +1))
            .groupBy('query').agg(f.sum('proportional_relevance').alias('dcg'),
                                  f.sum('ideal_proportional_relevance').alias('idcg')
                                  )
            .withColumn('ndcg', f.col('dcg')/f.col('idcg'))
            )
    return nDCG

parser = argparse.ArgumentParser()
parser.add_argument('--path_clicks', type=str, help='specify the path to the folder containing the click data to use.', required=True)
parser.add_argument('--path_query', type=str, help='specify the path to the json file containing the query.', required=True)
parser.add_argument('--rows_ndcg', type=int, help='the number of rows to run ndcg on.')
parser.add_argument('--output_path_rank_query', type=str, help='the number of rows to run ndcg on.')
parser.add_argument('--output_path_nDCG', type=str, help='the number of rows to run ndcg on.')
args = parser.parse_args()

path_query = args.path_query
path_clicks = args.path_clicks
rows_ndcg = 10
if args.rows_ndcg:
  rows_ndcg = args.rows_ndcg

query_stats = query_stats_on_clicks(path_query, path_clicks)
nDCG = query_stats.nDCG(rows_ndcg)
print('')
print('nDCG calculated over the first {} ranked products'.format(rows_ndcg))
print(nDCG.show(20, False))

print('')
print('Overview of the interactions given the submitted query')
print(query_stats.stats_query_clicks().show(20, False))

if args.output_path_rank_query:
  query_stats.stats_query_clicks().write.mode('overwrite').parquet(args.output_path_rank_query)
  print('')
  print('Saved stats_query_clicks in "{}"'.format(args.output_path_rank_query))
if args.output_path_nDCG:
  query_stats.nDCG(rows_ndcg).write.mode('overwrite').parquet(args.output_path_nDCG)
  print('Saved nDCG in "{}"'.format(args.output_path_nDCG))
