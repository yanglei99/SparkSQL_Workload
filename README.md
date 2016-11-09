# SparkSQL DataStore Benchmark on Mesos

## The Workload

The workload read data from Softlayer Object Storage [Reference](https://github.com/SparkTC/stocator), then write data using Spark SQL. 

* [Reference for high level overview](docs/SparkSQL_Workload.pdf)
* [Reference for the workload details in Spark SQL Python](python/workload.py)

### A sample run against ElasticSearch

	export marathonIp=MARATHON_IP
	curl -i -H 'Content-Type: application/json' -d@marathon-es.json $marathonIp:8080/v2/apps

* [Start ElasticSearch with Marathon Json](config/es.json)
* [Start workload with Marathon JSON](config/marathon-es.json)


### The Spark Mesos Docker Image 

The image is used for both Spark job submission and Spark executor on Mesos. [Reference Dockerfile](docker/Dockerfile)

* Built on top of mesosphere/mesos:1.0.0 image
* Install Spark 2.0.1 with Hadoop 2.7
* Install Java 8


