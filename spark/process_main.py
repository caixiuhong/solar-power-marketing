import os,sys
import time
import datetime
import configparser
from spark_processor import SparkProcessor





if __name__ == "__main__":


	start_ts=time.time()

	rooftop_main='s3a://xcai-s3/PV-rooftop/developable_planes'
	disposed_nsrdb_main='s3a://xcai-s3/disposed_nsrdb'


	#set configuration
	config = configparser.ConfigParser()
	config.read('/home/ubuntu/src/spark/config.ini')
	spark_master_node = config['server']['spark_master_node']
	postgres_instance = config['server']['postgres_instance']
	postgres_database = config['server']['postgres_database']
	postgres_username = config['server']['postgres_username']
	postgres_password = config['server']['postgres_password']

	
	# url of database to write data out
	url = 'jdbc:postgresql://{}:5432/{}'.format(postgres_instance, postgres_database)
	#print(url)
	
	#create a spark session
	processor=SparkProcessor(spark_master_node, 'SparkProcess')


	#join the rooftop data and solar radiation data for each year
	year_start=2005
	year_end=2005
	precision=4     # precision of geohash for join
	table='solar_power'		# table name in database to save
	
	for year in range(year_start, year_end+1):
		#file path of rooftop and solar radiation data
		rooftop_filepath='{}/*_{}/*'.format(rooftop_main,str(year)[-2:])
		disposed_nsrdb_path='{}/*{}.parquet'.format(disposed_nsrdb_main, str(year))
		#print(rooftop_filepath, disposed_nsrdb_path)
		print('processing year:', year)

		#check
		#test='s3a://xcai-s3/PV-rooftop/developable_planes/saltlakecity_ut_12/*'
		#rooftop_nsrdb= joinrooftop_nsrdb_byzip2(test,disposed_nsrdb_path, precision=precision)

		#join two tables using geohash by zip code
		rooftop_nsrdb= processor.joinrooftop_nsrdb_byzip2(rooftop_filepath,disposed_nsrdb_path, precision=precision)
		print('year {}: processing done.'.format(year))
		#rooftop_nsrdb.show(10)
		#print('count2:',rooftop_nsrdb.count())


		#print runtime
		print('run time: %fs' % (time.time()-start_ts))
		start_ts=time.time()
		#test=rooftop_nsrdb.limit(5).select('bldg_fid','solar_power')
		#test.show()
		
		#save data to database
		print('saving year:', year)
		processor.save_to_DB(rooftop_nsrdb, url, postgres_username, postgres_password, table, mode='append')
		print('saving done.')
		print('run time: %fs' % (time.time()-start_ts))
		



