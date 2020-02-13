#!/bin/sh

# request Bourne shell as shell for job
#$ -S /bin/sh

# The name of the job, can be anything, simply used when displaying the list of running jobs
#$ -N spark

# assume current working directory as paths
#$ -cwd

# Giving the name of the output log file
#$ -o run.log

# log of running errors
#$ -e error.log

# Now comes the commands to be executed
spark-submit \
--master yarn \
--deploy-mode client \
--executor-memory 6G \
--driver-memory 6G \
spark_ingest.py >run-ingest-02-13.log 
