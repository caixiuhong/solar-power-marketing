import os,sys
import time
import datetime
import configparser
from spark_ingester import SparkIngester


if __name__ == "__main__":

	start_ts=time.time()

	#read datasets on national solar radiation database 
	#nsrdb_path='s3://xcai-s3/nsrd/nsrdb'        # input path
	nsrdb_path='s3://nrel-pds-nsrdb/hdf5-source-files-v3/nsrdb'
	#small_test='s3://xcai-s3/test-folder/data.h5'     # test input path
	disposed_nsrdb_path='s3a://xcai-s3/disposed_nsrdb/disposed_nsrdb'   #output path          
	#disposed_nsrdb_path='s3a://xcai-s3/test-folder/disposed_nsrdb'     #test output path


	#set configuration
	config = configparser.ConfigParser()
	config.read('/home/ubuntu/src/spark/config.ini')
	spark_master_node = config['server']['spark_master_node']
	aws_key = config['server']['aws_key']
	aws_secretkey = config['server']['aws_secretkey']
	
	#create a spark session
	ingester=SparkIngester(spark_master_node, 'SparkIngest')


	year_start=2005       # read each file by each year
	year_end=2005
	meta_cols=['latitude','longitude']    # columns in meta dataset to extract
	ghi_cols=['ghi', 'year']              # columns in ghi dataset to extract
	meta='meta'							  # dataset name of meta: store the geospatial info
	ghi='ghi'							  # dataset name of ghi: store the ghi (irradiation) infor

	for year in range(year_start, year_end+1):
	
		nsrdb_file='{0}_{1}.h5'.format(nsrdb_path,str(year))       # input file name
		disposed_nsrdb_file='{0}_{1}.parquet'.format(disposed_nsrdb_path, str(year))   #output file name

		# extract data from h5 to parquet
		nsrdb_df= ingester.Hdf5ToParquet(nsrdb_file,meta, ghi, meta_cols,ghi_cols, \
			year, disposed_nsrdb_file, aws_key, aws_secretkey)   

		print('Job for year {0}: Done.'.format(year))
		#nsrdb_df.show(10)
		#print('count:', nsrdb_df.count())

	#print runtime
	print('run time: %fs' % (time.time()-start_ts))



