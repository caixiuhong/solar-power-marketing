import os,sys
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext, DataFrame, Row
from pyspark.sql.functions import avg, round
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from geo_processor import GeoProcessor

class SparkProcessor:
	def __init__(self, spark_master_node, appName):
		'''
		Join rooftoop table and solar radiation table, to calculate solar power, 
		and save the results in database, using spark.
		'''

		try:
			self.spark=SparkSession.builder.master(spark_master_node)\
			.appName(appName).config("spark.jars","/home/ubuntu/postgresql-42.2.9.jar").getOrCreate()
			self.spark.sparkContext.addPyFile("/home/ubuntu/src/spark/spark_processor.py")
			self.spark.sparkContext.addPyFile("/home/ubuntu/src/spark/geo_processor.py")

		except:
			print("Make sure the spark server is running!")
			sys.exit(1)

	def read_parquet(self, filepath):
		'''
		read parquet file to dataframe
		'''	

		df=self.spark.read.parquet(filepath)
		return df

	def save_to_DB(self, data, url,username, password,table, mode='overwrite'):
		'''
		use jdbc to write the data to postgresql
		'''

		data.write.format('jdbc') \
		.option("url", url) \
		.option("dbtable",table) \
		.option("user", username) \
		.option("password",password) \
		.option("driver", "org.postgresql.Driver") \
		.mode(mode).save()

		return

	def joinrooftop_nsrdb(self, rooftop_filepath, disposed_nsrdb_path, precision):
		'''
		join rooftop and solar radiation table with geohash
		by geomety of rooftop data and geospatial data of radiation data
		calculate solar power: area * ghi
		set solor power unit to be BWh
		output: rooftop planes and its relative solar power 
		'''
		
		#read rooftop data and solar radiatioin data
		rooftop_df=self.read_parquet(rooftop_filepath)
		disposed_nsrdb_df=self.read_parquet(disposed_nsrdb_path)

		#add geohash column to rooftop dataframe: transfer geometry_4326 to geohash
		geoprocessor=GeoProcessor()
		udf_GeomToGeohash=F.udf(lambda the_geom_4326: geoprocessor.GeomToGeohash(the_geom_4326, precision=precision))
		new_rooftop_df=rooftop_df.withColumn("geohash",udf_GeomToGeohash('the_geom_4326'))

		#add geohash column to solar radiation dataframe: transfer latitude and longitude to geohash
		udf_geohash=F.udf(lambda latitude, longitude: geoprocessor.GeoToGeohash(latitude,longitude, precision=precision))
		new_disposed_nsrdb_df=disposed_nsrdb_df.withColumn("geohash", udf_geohash('latitude','longitude'))



		new_rooftop_df.registerTempTable('new_rooftop_table')
		new_disposed_nsrdb_df.registerTempTable('new_disposed_nsrdb_table')

		#join rooftop and solar radiation table with geohash
		# calculate solar power: area * ghi
		# set solor power unit to be BWh
		rooftop_nsrdb= self.spark.sql( \
			"SELECT R.bldg_fid, R.year,R.zip,R.gid, R.city, R.state, 'United State' as country, \
			R.footprint_m2, R.flatarea_m2, R.slopearea_m2, R.the_geom_4326,\
			round(R.flatarea_m2+R.slopearea_m2,3) dev_area,\
			round((R.flatarea_m2+R.slopearea_m2)*S.avg_ghi/(10E8), 2) solar_power,\
			round(S.avg_ghi,2) avg_ghi \
			FROM new_rooftop_table R LEFT JOIN \
			(SELECT geohash, AVG(ghi) avg_ghi FROM new_disposed_nsrdb_table \
			GROUP BY geohash) S \
			ON R.geohash = S.geohash \
			")


		return rooftop_nsrdb


	def joinrooftop_nsrdb_byzip(self, rooftop_filepath, disposed_nsrdb_path, precision):
		'''
		join rooftop and solar radiation table with geohash
		by GEOMETRY of rooftop data and geospatial data of radiation data
		calculate solar power: area * ghi
		set solor power unit to be BWh
		output: zip code with its total solar power
		'''

		#read rooftop data and solar radiatioin data
		rooftop_df=self.read_parquet(rooftop_filepath)
		disposed_nsrdb_df=self.read_parquet(disposed_nsrdb_path)

		#udf: transfer geometry_4326 to geohash
		geoprocessor=GeoProcessor()
		udf_GeomToGeohash=F.udf(lambda the_geom_4326, zip: geoprocessor.GeomToGeohash(the_geom_4326, zip, precision=precision))
		
		#add geohash column to rooftop dataframe: transfer geometry_4326 to geohash
		new_rooftop_df=rooftop_df.withColumn("geohash",udf_GeomToGeohash('the_geom_4326', 'zip'))
		#new_rooftop_df.show(3)

		new_rooftop_df.registerTempTable('rooftop_table')
	
		#group the rooftop data by zip and geohash
		new_rooftop_df2=self.spark.sql(\
			"SELECT year, geohash, zip, city, state, 'United State' country, \
			SUM(footprint_m2) tot_footprint_m2, SUM(flatarea_m2) tot_flatarea_m2, \
			SUM(slopearea_m2) tot_slopearea_m2 \
			FROM rooftop_table GROUP BY year,zip,geohash,city,state")
	
		#new_rooftop_df2.show(3)


		#add geohash column to solar radiation dataframe: transfer latitude and longitude to geohash
		udf_GeoToGeohash=F.udf(lambda latitude, longitude: geoprocessor.GeoToGeohash(latitude, longitude,precision=precision))
		new_disposed_nsrdb_df= disposed_nsrdb_df.withColumn("geohash", udf_GeoToGeohash('latitude', 'longitude'))
		#new_disposed_nsrdb_df.show(3)

		new_disposed_nsrdb_df.registerTempTable('new_disposed_nsrdb_table')
		new_rooftop_df2.registerTempTable('new_rooftop_table')

		#join rooftop and solar radiation table with geohash
		# calculate solar power: area * ghi
		# set solor power unit to be BWh
		rooftop_nsrdb= self.spark.sql( \
			"SELECT R.geohash, R.year,R.zip, R.city, R.state, 'United States' as country, \
			R.tot_footprint_m2, R.tot_flatarea_m2, R.tot_slopearea_m2,\
			round(R.tot_flatarea_m2+R.tot_slopearea_m2,3) tot_developable_area,\
			round((R.tot_flatarea_m2+R.tot_slopearea_m2)*S.avg_ghi/(10E8), 2) solar_power,\
			round(S.avg_ghi,2) avg_ghi \
			FROM \
			new_rooftop_table R \
			LEFT JOIN \
			(SELECT year, geohash, AVG(ghi) avg_ghi FROM new_disposed_nsrdb_table \
			GROUP BY geohash,year) S \
			ON R.geohash = S.geohash and R.year = S.year \
			")
	

		return rooftop_nsrdb
	

	def joinrooftop_nsrdb_byzip2(self, rooftop_filepath, disposed_nsrdb_path, precision):
		'''
		join rooftop and solar radiation table with geohash
		by ZIP of rooftop data and geospatial data of radiation data
		calculate solar power: area * ghi
		set solor power unit to be BWh
		output: zip code with its total solar power
		'''

		#read rooftop data and solar radiatioin data
		rooftop_df=self.read_parquet(rooftop_filepath)
		disposed_nsrdb_df=self.read_parquet(disposed_nsrdb_path)
	
		#drop reduntant columns of rooftop data, reduce columns of rooftop dataframe
		new_rooftop_df= rooftop_df.select('bldg_fid','footprint_m2','slopearea_m2','flatarea_m2','zip','city','state','year')
	
		new_rooftop_df.registerTempTable('rooftop_table')
		
		#group the rooftop data by zip, reduce rows of rooftop dataframe
		new_rooftop_df2=self.spark.sql(\
			"SELECT year, zip, city, state, 'United States' country, \
			SUM(footprint_m2) tot_footprint_m2, SUM(flatarea_m2) tot_flatarea_m2, \
			SUM(slopearea_m2) tot_slopearea_m2 \
			FROM rooftop_table GROUP BY year,zip,city,state")
	
		#add geohash column to rooftop dataframe: transfer zipcode to geohash
		geoprocessor=GeoProcessor()
		udf_ZipToGeohash=F.udf(lambda zipcode: geoprocessor.ZipToGeohash2(zipcode, precision=precision))
		new_rooftop_df3= new_rooftop_df2.withColumn("geohash",udf_ZipToGeohash('zip'))
		#udf_ZipToGeohash=F.udf(lambda x: GeoToGeohash(x[0],x[1],precision=precision))
		#new_rooftop_df4= new_rooftop_df3.withColumn("geohash",udf_ZipToGeohash("(lat,lng)"))

		#add geohash column to solar radiation dataframe: transfer latitude and longitude to geohash
		udf_GeoToGeohash=F.udf(lambda latitude, longitude: geoprocessor.GeoToGeohash(latitude, longitude,precision=precision))
		#new_rooftop_df4= new_rooftop_df3.withColumn("geohash",udf_GeoToGeohash('latitude','longitude'))
		new_disposed_nsrdb_df= disposed_nsrdb_df.withColumn("geohash", udf_GeoToGeohash('latitude', 'longitude'))
	

		new_disposed_nsrdb_df.registerTempTable('new_disposed_nsrdb_table')
		new_rooftop_df3.registerTempTable('new_rooftop_table')
		#new_disposed_nsrdb_df.filter(new_disposed_nsrdb_df.geohash=='u09t').show(3)


		#join rooftop and solar radiation table with geohash
		# calculate solar power: area * ghi
		# set solor power unit to be BWh
		rooftop_nsrdb= self.spark.sql( \
			"SELECT R.geohash, R.year,R.zip, R.city, R.state, R.country, \
			R.tot_footprint_m2, R.tot_flatarea_m2, R.tot_slopearea_m2,\
			R.tot_flatarea_m2+R.tot_slopearea_m2 tot_developable_area,\
			(R.tot_flatarea_m2+R.tot_slopearea_m2)*S.avg_ghi/(10E8) solar_power,\
			S.avg_ghi avg_ghi \
			FROM new_rooftop_table R \
			LEFT JOIN \
			(SELECT year, geohash, AVG(ghi) avg_ghi FROM new_disposed_nsrdb_table \
			GROUP BY geohash,year) S \
			ON R.geohash = S.geohash and R.year = S.year \
			")
	

		return rooftop_nsrdb

