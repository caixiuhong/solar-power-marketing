from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
class read_s3file:
	

	def read_parquet(self, sc,filename, bucketname):
		sqlcontext=SQLContext(sc)
		df=sqlcontext.read.parquet("s3a://"+bucketname+'/'+filename, header=True, inferSchema=True)
		return df

	def read_hdf(self, sc, filepath, input_schema):
		sqlcontext=SQLContext(sc)
		df=sqlcontext.read.hdf()

