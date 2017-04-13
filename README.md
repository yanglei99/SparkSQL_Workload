# SparkSQL DataStore Benchmark on Mesos

## The Workload

The workload read data from Softlayer Object Storage with [Spark Swift integration](https://github.com/SparkTC/stocator), then write data to a DataStore using Spark SQL. The run results, throughput and latency, are stored as CSV file in Softlayer Object Storage too.

* Reference [the high level overview](docs/SparkSQL_Workload.pdf)
* Reference [the workload details in Python using Spark SQL](python/workload.py)

### A sample run against ElasticSearch

	export marathonIp=MARATHON_IP
	curl -i -H 'Content-Type: application/json' -d@marathon-es.json $marathonIp:8080/v2/apps

* [Start ElasticSearch with Marathon JSON](config/es.json)
* [Start workload with Marathon JSON](config/marathon-es.json)


### The Spark Mesos Docker Image 

The image is used for both Spark job submission and Spark executor on Mesos, besides it can be used to start Spark Standalone cluster. 

[Reference](config/spark-cluster/README.md)
