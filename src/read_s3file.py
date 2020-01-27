import os,sys
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext, DataFrame
from pyspark import SparkConf, SparkContext
import h5py
import s3fs
import pandas as pd
import numpy as np


#os.environ['PYSPARK_SUBMITT_ARGS']='--packages org.postgresql:postgresql:42.2.5 pyspark-shell'
#set the environment of spark
try:
	spark=SparkSession.builder.master("spark://ec2-54-210-216-145.compute-1.amazonaws.com:7077")\
	.appName('My Spark Application').config("spark.jars","/home/ubuntu/postgresql-42.2.9.jar").getOrCreate()
except:
	print("Make sure the spark server is running!")
	sys.exit(1)
	

def read_parquet(filepath):
	#read parquet file: PV-rooftop datasets
	
	df=spark.read.parquet(filepath)

	return df

def read_hdf5(filepath):
	#read hdf5 file: solar-radiation datasets
	'''
	s3=s3fs.S3FileSystem()
	def readchunk(v):
		empty=h5py.File(s3.open(filepath))
		return empty['/'][v,:]
	foo=spark.parallelize(range(0,100)).map(lambda v: readchunk(v))
	print(foo.count())
	'''

	s3=s3fs.S3FileSystem()
	f=h5py.File(s3.open(filepath),'r')
	print(f.keys())
	#print(f.keys())

	return 

def read_hdf5_pd(filepath):
	#read hdf5 file: solar-radiation datasets: use pandas
	s3=s3fs.S3FileSystem()
	f=pd.read_hdf(s3.open(filepath),'r')
	for key in f.keys():
		print(f[key].name)
		print(f[key].shape)

	return f

def calculate_data(file):
	#calculating solar power
	pass

def ziptogeohash(zipcode):
	#transfer zipcode to geohash 
	pass


def write_hdf5_file(filepath):
	#write testing hdf5 file for testing: data.h5
	d1 = np.random.random(size = (1000,20))
	d2 = np.random.random(size = (1000,200))
	hf=h5py.File('data.h5','w')
	hf.create_dataset('dataset_1', data=d1)
	hf.create_dataset('dataset_2', data=d2)
	hf.close()
	return


def write_to_DB(df, table, mode):
	#write data to postgresql
	url = 'jdbc:postgresql://ec2-18-206-8-25.compute-1.amazonaws.com:5432/words'
	properties= {'user':'db_select', 'password': "01222022", 'driver': "org.postgresql.Driver"}
		

	try:
		df.write.jdbc(url=url, table = table, mode=mode, properties= properties)

	except:
		print("Make sure the database server is running!")
		sys.exit(1)


def save_to_DB(data, url,username, password,table, mode='overwrite'):
	#use another commond to write the data to postgresql
    data.write.format('jdbc') \
    .option("url", url) \
    .option("dbtable",table) \
    .option("user", username) \
    .option("password",password) \
    .option("driver", "org.postgresql.Driver") \
    .mode(mode).save()



import time
import datetime
start_ts=time.time()

#read datasets on rooftop
rooftop_filepath='s3a://xcai-s3/PV-rooftop/developable_planes/albany_ny_*/*'
rooftop_df=read_parquet(rooftop_filepath)
print(rooftop_df.show(2))

#read datasets on national solar radiation database 
#nsrdb_filepath='s3://xcai-s3/nsrd/nsrdb_2006.h5'
#nsrdb_df=read_hdf5(nsrdb_filepath)
partitions=4
test='s3://nrel-pds-wtk/hdf5-source-files-gridded/2007-01.h5'
#write_hdf5_file('./')

#nsrdb_df=read_hdf5(test)

#for df in rooftop_df:
#	df.createOrReplaceTempView("parquetFile")
#	print(spark.sql('SELECT city FROM df where city = NY'))
	#df.printSchema()
	#df.show(3)
#	df.count()
#for df in nsrdb_df:
#	df.printSchema()
#	df.show(3)
#	df.count()


#writing rooftop data to database
instance = 'ec2-18-206-8-25.compute-1.amazonaws.com'
database = 'words'
username = 'db_select'
password = '01222020'

url = 'jdbc:postgresql://{}:5432/{}'.format(instance, database)

table = 'rooftop'
rooftop_df.createOrReplaceTempView("rooftop_df")
select_rooftop=spark.sql("SELECT bldg_fid, footprint_m2,zip, city, state, year FROM rooftop_df ")

save_to_DB(select_rooftop, url, username, password, table)

#write_to_DB(rooftop_df.limit(5), 'rooftop','overwrite')
#write_to_DB(nsrdb, 'nsrdb', 'overwrite')

#print runtime
print('run time: %f s') % (time.time()-start_ts)


