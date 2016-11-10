#*******************************************************************************
# Copyright (c) 2016 IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#******************************************************************************/
#
# A little more explanation beyond:  loading file into DataFrame, measure the save latency and throughout, save the results in csv
#     - Repeat: indicates a total number of DataFrame to save. 
#     - Sample: indicates the sample ratio for a given file. The max DataFrame created for a given file is int(1/sample)
#     - FileNameSuffixPattern: used when Repeat > Max DataFrame per file. The additional files are loaded with suffix of fileNameSuffixPattern.replace("index",nextFileIndex).
#     - PartitionNum: the partition number the DF 
#
# To Execute:
#
#    spark-submit --packages com.databricks:spark-csv_2.11:1.5.0,org.elasticsearch:elasticsearch-spark-20_2.11:5.0.0 --jars stocator-1.0.8-SNAPSHOT-jar-with-dependencies.jar --conf spark.es.nodes=127.0.0.1 workload.py
#    spark-submit --packages com.databricks:spark-csv_2.11:1.5.0,org.elasticsearch:elasticsearch-spark-20_2.11:5.0.0 --jars stocator-1.0.8-SNAPSHOT-jar-with-dependencies.jar --conf spark.es.nodes=127.0.0.1 workload.py put
#
#    curl -i -H 'Content-Type: application/json' -d@marathon-es.json http://169.44.129.70:8080/v2/apps
#
# About schema:
#
# ES:
#     curl -XPUT 'http://127.0.0.1:9200/spark/' -d '{ "settings" : { "index" : { "number_of_shards" : 3, "number_of_replicas" : 2 } }, "mappings" :{"workload":{ "properties": {"timestamp": {"type": "date" }}}}}'
#

import pprint
import os, sys
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from telnetlib import AUTHENTICATION
import uuid

print 'Number of arguments:', len(sys.argv), 'arguments.'
print 'Argument List:', str(sys.argv)

fname = "data/put"
if (len(sys.argv)>1):
    fname= sys.argv[1]
    

format=os.getenv("FORMAT","FORMAT")
datastore=os.getenv("DATASTORE","workload")
sample=float(os.getenv("SAMPLE_RATIO","1"))
repeat=int(os.getenv("REPEAT","1"))
seed=int(os.getenv("SEED","42"))
startIndex=int(os.getenv("START_INDEX",0))
fileNameSuffixPattern=os.getenv("FILENAME_SUFFIX_PATTERN","(index)")
interval=int(os.getenv("INTERVAL","10"))
preload=os.getenv("PRELOAD","true")
partitionNum=int(os.getenv("PARTITION_NUM","2"))

conf = SparkConf().setAppName("Workload in Spark SQL Python for "+format +" to "+datastore+"start at "+str(startIndex)+" iterate "+  str(repeat) +" times with sampling " +str(sample) +" of seed "+str(seed) +" in partitions:" +str(partitionNum))

# setup swift configuration

st_user=os.getenv("ST_USER","ACCOUNT:USER").split(":")
apikey=os.getenv("ST_KEY","API_KEY")
authurl=os.getenv("ST_AUTH","https://sjc01.objectstorage.softlayer.net/auth/v1.0")
container=os.getenv("ST_CONTAINER","YOUR_CONTAINER")

tenant = st_user[0]
username = st_user[1]

from urlparse import urlparse
o = urlparse(authurl)

datacenter = o.netloc.split('.')[0]

filepath = "swift2d://"+container+"."+datacenter+"/"

print "auth_url:", authurl,",tenant:", tenant, ",user:", username, ",filepath:", filepath, ", filename:", fname

sc = SparkContext(conf=conf)

sc._jsc.hadoopConfiguration().set("fs.swift2d.impl","com.ibm.stocator.fs.ObjectStoreFileSystem")
#sc._jsc.hadoopConfiguration().set("fs.swift2d.block.size",1024*1024)
sc._jsc.hadoopConfiguration().set("fs.swift2d.service."+datacenter+".auth.url",authurl)
sc._jsc.hadoopConfiguration().set("fs.swift2d.service."+datacenter+".public","true")
sc._jsc.hadoopConfiguration().set("fs.swift2d.service."+datacenter+".tenant",tenant)
sc._jsc.hadoopConfiguration().set("fs.swift2d.service."+datacenter+".password",apikey)
sc._jsc.hadoopConfiguration().set("fs.swift2d.service."+datacenter+".username",username)
sc._jsc.hadoopConfiguration().set("fs.swift2d.service."+datacenter+".auth.method","swiftauth")

sqlContext = SQLContext(sc)


def getFileName(currentFileIndex):
    if (currentFileIndex==0):
        return filepath+fname
    return filepath+fname+fileNameSuffixPattern.replace("index",str(currentFileIndex))
    
def getSplits(dataFrame, maxSplits):
    splits = []
    remaining = 1
    currentSplit=0
    while (remaining > 0 and currentSplit<maxSplits):
        if (sample <= remaining):
            splits.append(sample)
        else:
            splits.append(remaining)
        remaining -= sample
        currentSplit += 1
    if (remaining>0):
            splits.append(remaining)
    splitsDF = dataFrame.randomSplit(splits, seed)   
    if (remaining != 0 ):
        splitsDF.pop()
    return splitsDF


def writeDF(sampleDF):
    import time
    if (preload):
        print 'caching the dataframe'
        begin_ts = time.time()
        sampleDF.cache() 
        count = sampleDF.count() # Force load
        duration= time.time()-begin_ts
        rate = count / duration
        print "Cache count:", count, "/duration:" ,duration, "=", rate, "/s"
    print 'saving the dataframe'
    # Need special treatment to Cassandra keyspace and table
    name = datastore.split(":")
    begin_ts = time.time()
    if (len(name) == 1 ):
        sampleDF.write.mode('append').format(format).save(name[0])
    else:
        sampleDF.write.mode('append').format(format).option("table",name[1]).option("keyspace",name[0]).save()
        
    duration = time.time()-begin_ts
    rate = count / duration
    print "Save count:", count, "/duration:" ,duration, "=", rate, "/s"
    return (count, duration, rate)


remaining = repeat
currentFileIndex = 0
throughputResult = []

while (remaining > 0 ):

    filename = getFileName(currentFileIndex)
    print '... about to load: ',filename
    
    df = sqlContext.read.format("json").load(filename)
    df.printSchema()
    df.show()
    
    splitDF= getSplits(df,remaining)
    splitTotal = len(splitDF)
    for i in range(splitTotal):
        aDF=splitDF[i]
        totalRecords= aDF.count()
        totalPartitions = aDF.rdd.getNumPartitions()

        if (partitionNum != totalPartitions):
            aDF = aDF.repartition(partitionNum)
            totalPartitions = aDF.rdd.getNumPartitions()
    
        print '[',repeat-remaining+1, '/', repeat, ']',' total records: ', totalRecords, ' in total partition', totalPartitions
    
        throughputResult.append(writeDF(aDF))
        remaining -= 1
    
    currentFileIndex +=1

throughputDF =  sqlContext.createDataFrame(throughputResult, ['count', 'duration', 'throughput'])
throughputDF.printSchema()
throughputDF.show()
throughputDF = throughputDF.repartition(1)

print '... about to write result in total partition:', throughputDF.rdd.getNumPartitions()
throughputDF.write.format("com.databricks.spark.csv").option("header", "true").save(filepath+"result/"+str(uuid.uuid1())+".csv")


sc.stop()
