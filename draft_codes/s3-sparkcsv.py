import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
#from configparser import ConfigParser

# set up spark context
# sc = SparkContext(conf=SparkConf().setAppName("se"))
# spark = SparkSession.builder.appName("se").getOrCreate()
#config = ConfigParser()
#spark = SparkSession.builder.appName('census').getOrCreate()

try:
    spark=SparkSession.builder.master("spark://ec2-54-210-216-145.compute-1.amazonaws.com:7077")\
    .appName('My Spark Application').config("spark.jars","/home/ubuntu/postgresql-42.2.9.jar").getOrCreate()
except:
    print("Make sure the spark server is running!")
    sys.exit(1)


# read data:

# path = 's3a://enjoyablecat/unigram_freq.csv'
path = 's3a://xcai-s3/test-folder/unigram_freq.csv'

csv = spark.read.csv(path, header=True)
csv.show()

# database
def save(data, url, username, password, table, mode='append'):
    data.write.format('jdbc') \
    .option("url", url) \
    .option("dbtable",table) \
    .option("user", username) \
    .option("password",password) \
    .option("driver", "org.postgresql.Driver") \
    .mode(mode).save()

instance = 'ec2-18-206-8-25.compute-1.amazonaws.com'
database = 'words'
username = 'db_select'
password = '01222020'

url = 'jdbc:postgresql://{}:5432/{}'.format(instance, database)

# table = 'word_freq'
table = 'census_2017'

save(csv, url, username, password, table)
