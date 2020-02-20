import os,sys
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext, DataFrame
from pyspark import SparkConf, SparkContext
import s3fs
import h5py

class SparkIngester:
	def __init__(self, spark_master_node, appName):
		'''
		Transfer HDF file to parquet using spark.
		'''
		try:
			self.spark=SparkSession.builder.master(spark_master_node)\
			.appName(appName).config("spark.jars","/home/ubuntu/postgresql-42.2.9.jar").getOrCreate()
			self.spark.sparkContext.addPyFile("/home/ubuntu/src/spark/spark_ingester.py")
	
		except:
			print("Make sure the spark server is running!")
			sys.exit(1)


	def Hdf5ToParquet(self,filepath,meta, ghi, meta_cols,ghi_cols, year, output_path,aws_key, aws_secretkey,mode='errorifexists'):
		'''
		aggreage meta dataset and ghi dataset into spark RDD.
		read geospatial location information from h5 file meta dataset
		sum total irration by year for each geospacial location from ghi dataset
		save them to parquet file in s3 bucket
		'''
		s3=s3fs.S3FileSystem()
		_h5file=h5py.File(s3.open(filepath),'r')
	
		loc_n1=_h5file[meta].shape[0]
		loc_n2=_h5file[ghi].shape[1]
		scale_factor=_h5file[ghi].attrs['psm_scale_factor']
		chunk_size=1000    # read ghi for locations in a chunk size
		n_chunk=loc_n1//chunk_size+1  # number of chunk to get all locations ghi
		#n_chunk=10

		if loc_n1 != loc_n2:
			#check if location numbers are the same in the two datasets
			print('Warning: the location numbers in {0}/{1} and {0}/{2} are not the same.' \
			.format(filepath, meta, ghi))
			sys.exit(1)
		#print(loc_n1,loc_n2)


		def readchunk(v,meta,ghi, year, aws_key, aws_secretkey):
			s3=s3fs.S3FileSystem(anon=False, key=aws_key , secret=aws_secretkey)
			#s3=s3fs.S3FileSystem()
			_h5file=h5py.File(s3.open(filepath),'r')
			meta_chunk=_h5file[meta][v*chunk_size:(v+1)*chunk_size]    # read meta dataset: geospatial info for each location
			ghi_chunk=_h5file[ghi][:,v*chunk_size:(v+1)*chunk_size]\
			.sum(axis=0)/scale_factor                                  # read ghi dataset: ghi info each year for each location
			meta_l=[]

			def convertNptype(l):
				'''
				convert numpy type of data to python type. 
				because spark rdd does not recognize numpy type
				'''
				res=getattr(l, "tolist", lambda: value)()
				return res

			meta_l=  [ convertNptype(meta_chunk[x])  for x in meta_cols]
			ghi_l = convertNptype(ghi_chunk)
			agg_l=meta_l+[ghi_l]+[[year]*len(ghi_l)]          # aggregate meta and ghi data together

			return list(map(list, zip(*agg_l)))

		#print('test:',loc_n1,loc_n2)
		df=self.spark.sparkContext.parallelize(range(0,n_chunk)) \
		.flatMap(lambda v: readchunk(v,meta,ghi, year, aws_key, aws_secretkey))\
		.toDF(meta_cols+ghi_cols)                   # parallelize to read each chunk 

	
		df.write.parquet(output_path,mode=mode)       # write the dataframe to parquet format

		return df