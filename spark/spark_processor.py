import os,sys
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext, DataFrame, Row
from pyspark.sql.functions import avg, round
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pygeocoder import Geocoder
import pygeohash as pgh
import shapely
from shapely import wkt
from uszipcode import SearchEngine
import pgeocode
import geohash2



class SparkProcessor:
	def __init__(self, spark_master_node, appName):
		try:
			self.spark=SparkSession.builder.master(spark_master_node)\
			.appName(appName).config("spark.jars","/home/ubuntu/postgresql-42.2.9.jar").getOrCreate()

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
		udf_GeomToGeohash=F.udf(lambda the_geom_4326: self.GeomToGeohash(the_geom_4326, precision=precision))
		new_rooftop_df=rooftop_df.withColumn("geohash",udf_GeomToGeohash('the_geom_4326'))

		#add geohash column to solar radiation dataframe: transfer latitude and longitude to geohash
		udf_geohash=F.udf(lambda latitude, longitude: pgh.encode(latitude,longitude, precision=precision))
		new_disposed_nsrdb_df=disposed_nsrdb_df.withColumn("geohash", udf_geohash('latitude','longitude'))

		#udf_GeoToZip = F.udf(lambda latitude, longitude: GeoToZip(latitude,longitude))
		#new_disposed_nsrdb_df=new_disposed_nsrdb_df.withColumn("zip", udf_GeoToZip('latitude','longitude'))
		#new_disposed_nsrdb_df=disposed_nsrdb_df.rdd.map(lambda row: GeoToZip(row['latitude'], row['longitude']))


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
		udf_GeomToGeohash=F.udf(lambda the_geom_4326, zip: self.GeomToGeohash(the_geom_4326, zip, precision=precision))

		#udf: transfer zipcode to geohash
		udf_ZipToGeohash=F.udf(lambda zipcode: self.ZipToGeohash(zipcode,precision=precision))

		#add geohash column to rooftop dataframe: transfer zipcode to geohash
		#new_rooftop_df= rooftop_df.select('bldg_fid','footprint_m2','slopearea_m2','flatarea_m2','zip','city','state','year')\
		#.withColumn("geohash",udf_ZipToGeohash('zip'))
		
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
		udf_GeoToGeohash=F.udf(lambda latitude, longitude: self.GeoToGeohash(latitude, longitude,precision=precision))
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
		udf_ZipToGeohash=F.udf(lambda zipcode: GeoProcess.ZipToGeohash2(zipcode, precision=precision))
		new_rooftop_df3= new_rooftop_df2.withColumn("geohash",udf_ZipToGeohash('zip'))
		#udf_ZipToGeohash=F.udf(lambda x: GeoToGeohash(x[0],x[1],precision=precision))
		#new_rooftop_df4= new_rooftop_df3.withColumn("geohash",udf_ZipToGeohash("(lat,lng)"))

		#add geohash column to solar radiation dataframe: transfer latitude and longitude to geohash
		#udf_GeoToGeohash=F.udf(lambda latitude, longitude: GeoToGeohash(latitude, longitude,precision=precision))
		udf_GeoToGeohash=F.udf(lambda latitude, longitude: pgh.encode(latitude,longitude, precision=precision))
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

class GeoProcess:
	def __init__(self):

		self.search=None  # to store the search engine for uszipcode, avoid to run search engine every time in worker nodes
	
	def GeoToZip(self, latitude,longitude):
		'''
		use uszipcode to transfer latitude and longitude to zipcode 
		'''
		#option: google map api, need googlemap api key. has request limit
		#geoinfo=Geocoder.reverse_geocode(latitude, longitude) 
		#return geoinfo.postal_code


		if not self.search:
			self.search = SearchEngine()
			result = search.by_coordinates(latitude, longitude, returns=1)
	
		return result[0].zipcode  if result else None

	
	def ZipToGeohash(self, zipcode, precision=4):
		'''
		Use uszipcode to transfer latitude and longitude to zipcode
		Use pygeohash to transfer zipcode to geohash
		precision: define the precision of geohash 
		'''
		
		if not self.search:
			self.search = SearchEngine()
		result = search.by_zipcode(zipcode)
	
		res=pgh.encode(result.lat,result.lng, precision=precision)
	
		return res 

	
	def ZipToGeohash2(self, zipcode, precision=4):
		'''
		Use pgeocode to transfer latitude and longitude to zipcode
		Use geohash2 to transfer zipcode to geohash
		precision: define the precision of geohash 
		'''

		nomi = pgeocode.Nominatim('us')
		info=nomi.query_postal_code(zipcode)
		latitude, longitude=info.latitude, info.longitude
		try:
			geohash=geohash2.encode(info.latitude, info.longitude, precision)
		except:
			geohash=None      #in case there are no zip code for input data
		return geohash

	
	def GeoToGeohash(self, latitude,longitude, precision=4):
		'''
		Use pygeohash to transfer latitude and longitude to goehash
		precision: define the precision of geohash 
		'''

		result=pgh.encode(latitude,longitude, precision=precision)

		return result 
	
	def GeomToGeohash(self, the_geom_4326, zip, precision=4):
		'''
		transfer geometry of EPSG 4326 to geohash
		use shapely to get the centroid of geometry
		use pygeohash to transfere latitude and longitude of centroid to geohash
		if geometry to geohash is not successful, use zip code instead
		'''

		try:   # transfer geometry to geohash
			geometry=shapely.wkt.loads(the_geom_4326)
			if geometry.geom_type == 'MultiPolygon':    # special case for MultiPolygon type geometry
				latitude,longitude=geometry.bounds[1], geometry.bounds[0]
			else:
				latitude, longitude= geometry.centroid.y, geometry.centroid.x
			res=pgh.encode(latitude,longitude, precision=precision)
		except:  # if geometry not successful, use zip code instead
			try:
				res=self.ZipToGeohash(zip,precision=precision)
			except:
				res=None   # if zip code is null
		return res

	def GeomToLatlong(self,the_geom_4326, precision=4):
		'''
		transfer geometry of EPSG 4326 to latitude, longitude
		use shapely to output centroid of geometry
		'''
		geometry=shapely.wkt.loads(the_geom_4326)
		latitude, longitude= geometry.centroid.y, geometry.centroid.x
		return latitude, longitude
	
