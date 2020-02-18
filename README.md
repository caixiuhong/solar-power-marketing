# Solar Power Marketing 
This project is to build up a pipeline to analyze urban solar power market in US, counpling with PV rooftop dataset and solar radiation dataset.

# Overview
Solar Power is renewable and clean for earth. The market potential in US is booming and huge. The urban rooftops provide huge potential to utilize solar power and save a lot of energy, utilizing the PV rooftop and solar radiation databases, this project will provide where in US has most potential to build up solar pannels and generate large solar power in zip code level.

features:
- 2005-2014 PV rooftop datasets
- Same-year solar radiation data
- Deep to zip code level solar power marketing

Solar-power-marketing is scalable and built on Amazon S3, EC2, Spark and PostgreSQL on AWS. Tableau can be used to visualize and analyze the standardized dataset.

[Slides](https://drive.google.com/open?id=1QFfqpmwcNVOsM8dnTihsSDQ28R_BQJEHRMxpEA28v6w)

![Pipeline](demo/pipeline.png)

# Requirments
* Spark
* PostgreSQL
* Airflow
* Python 3.6
* Python libs: h5py, s3fs, pygeohash, shapely, uszipcode, pgeocode, geohash2
* Amazon AWS Account

## Environment set up
Install python 3.6 

```bash
$ sudo apt-get install software-properties-common
$ sudo add-apt-repository ppa:deadsnakes/ppa 
$ sudo apt-get update
$ sudo apt-get install python3.6
```

Set python 3.6 as default python

```bash
$ sudo update-alternatives --install /usr/bin/python python /usr/bin/python3.6 1
```

Update pip

```bash
$ sudo pip install --upgrade pip
```

Install s3fs

```bash
$ pip install awscli --upgrade –user
$ pip install boto3
$ pip install s3fs
```

Install h5py

```bash
$ sudo pip install cython
$ sudo apt-get install libhdf5-dev
$ sudo pip install h5py
```

Install Airflow

```bash
$ export AIRFLOW_HOME=~/airflow
$ sudo apt install python3.6-dev
$ pip install apache-airflow
$ airflow initdb
$ airflow webserver -p 5050
```

# Installation
Clone the Solar-power-marketing project to your local computer or `m4.4xlarge` EC2 instance and install awscli and other requirements.

```bash
$ git clone https://github.com/caixiuhong/solar-power-marketing.git
```

## Mannually ingestion and process.
Ingest solar radiation data into S3 bucket, using Spark.

```bash
$ ./src/spark/submit-ingest.sh
```

Process PV rooftop data and solar radiation data using Spark, and save results in PostgreSQL.

```bash
$ ./src/spark/submit-process.sh
```

## Use Airflow for ingestion and process. Trigger solar_power_marketing dag.
```bash
$ airflow scheduler
```

# Getting Started

Open the website at [http://www.datagourmet.xyz](http://www.datagourmet.xyz). Three graphs are shown in the interactive dashboard. On the top is the market size of solar power in chosen year over the states. Bottom left shows the relative cities that have most solar power markets for the whole nation or chosen state. Bottom right shows the relative zip code region that have most solar power market for the whole nation or chosen state/city. 

# Credits

Solar-Power-Marketing was built as a project at Insight Data Engineering in the Jan 2020 session by Xiuhong Cai. It is availble as open source and is free to use and modify by anyone.
