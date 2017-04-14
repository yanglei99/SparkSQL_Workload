#*******************************************************************************
# Copyright (c) 2017 IBM Corp.
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
#     - REPEAT: indicates a total number of DataFrame to save. 
#     - SAMPLE_RATIO: indicates the sample ratio for a given file. The max DataFrame created for a given file is int(1/sample)
#     - FILENAME_PATTERN: used when Repeat > Max DataFrame per file. The additional files are loaded using .format(filename, str(index))
#     - PARTITION_NUM: the DataFrame partitions. -1 will rely on source loader
#     - SCHEMA: a space separated string of fieldName:fieldType which will be used to calculate schema. 
#
# To Execute, an example for ES
#
#    spark-submit --packages org.elasticsearch:elasticsearch-spark-20_2.11:5.0.0,com.databricks:spark-csv_2.11:1.5.0,com.ibm.stocator:stocator:1.0.1 --conf spark.es.nodes=127.0.0.1 workload.py filename
#
#    On Mesos:
#
#    start ES from docker
#
#    curl -i -H 'Content-Type: application/json' -d@es.json http://$marathonIp:8080/v2/apps
#
#    run workload
#
#    curl -i -H 'Content-Type: application/json' -d@marathon-es.json http://$marathonIp:8080/v2/apps 
#    or
#    curl -i -H 'Content-Type: application/json' -d@marathon-es-index.json http://$marathonIp:8080/v2/apps
#
#    About schema (default password changeme):
#
#     curl -XPUT -u elastic 'http://'"$esIp"':9200/spark/' -d '{"settings" : { "index" : { "number_of_shards" : 3, "number_of_replicas" : 2 } }, "mappings" :{"workload":{ "properties": {"timestamp": {"type": "date" }}}}}'
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

if (len(sys.argv)<=1):
    print 'need filename'
    sys.exit(1)

fname= sys.argv[1]
format=os.getenv("FORMAT","FORMAT")
datastore=os.getenv("DATASTORE","workload")
sample=float(os.getenv("SAMPLE_RATIO","1"))
repeat=int(os.getenv("REPEAT","1"))
seed=int(os.getenv("SEED","42"))
startIndex=int(os.getenv("START_INDEX",0))
fileNamePattern=os.getenv("FILENAME_PATTERN","{0}")
interval=int(os.getenv("INTERVAL","10"))
preload=os.getenv("PRELOAD","true")
partitionNum=int(os.getenv("PARTITION_NUM","-1"))
schemaStr=os.getenv("SCHEMA","")

schema = None

if (len(schemaStr)>0):
    print "calculate schema ..."
    fields = []
    for fieldTypeStr in schemaStr.split(' '):
        fileTypeTuple = fieldTypeStr.split(':')
        fieldName = fileTypeTuple[0]
        fieldType = fileTypeTuple[1]
        if (fieldType == "long"):
            field = StructField(fieldName, LongType(), True)
        elif (fieldType == "short"):
            field = StructField(fieldName, ShortType(), True)
        elif (fieldType == "integer"):
            field = StructField(fieldName, IntegerType(), True)
        elif (fieldType == "boolean"):
            field = StructField(fieldName, BooleanType(), True)
        elif (fieldType == "string"):
            field = StructField(fieldName, StringType(), True)
        elif (fieldType == "date"):
            field = StructField(fieldName, DateType(), True)
        elif (fieldType == "timestamp"):
            field = StructField(fieldName, TimestampType(), True)
        elif (fieldType == "double"):
            field = StructField(fieldName, DoubleType(), True)
        elif (fieldType == "decimal"):
            field = StructField(fieldName, DecimalType(), True)
        elif (fieldType == "float"):
            field = StructField(fieldName, FloatType(), True)
        elif (fieldType == "byte"):
            field = StructField(fieldName, ByteType(), True)
        else:
            field = StructField(fieldName, StringType(), True)
        fields.append(field)     
            
    schema = StructType(fields)

print "Workload in Spark SQL Python for ", format, " to ", datastore , "start at ", str(startIndex), " iterate ", str(repeat), " times with sampling ", str(sample), " of seed ", str(seed), " in partitions ", str(partitionNum)

conf = SparkConf().setAppName("Workload in Spark SQL Python")

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
    if (currentFileIndex!=0):
        tname = fileNamePattern.format(fname,str(currentFileIndex))
    else:
        tname = fname
    return filepath+tname
    
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

showSchema = True

while (remaining > 0 ):

    filename = getFileName(currentFileIndex)
    print '... about to load: ',filename
    
    if (schema):
        df = sqlContext.read.format("json").option("mode","DROPMALFORMED").load(filename, schema=schema)
    else:
        df = sqlContext.read.format("json").load(filename)
    
    splitDF= getSplits(df,remaining)
    splitTotal = len(splitDF)
    for i in range(splitTotal):
        aDF=splitDF[i]
        
        if (showSchema):
            aDF.printSchema()
            aDF.show()
            showSchema = False

        totalPartitions = aDF.rdd.getNumPartitions()
        if (partitionNum>0 and partitionNum != totalPartitions):
            aDF = aDF.repartition(partitionNum)
            totalPartitions = aDF.rdd.getNumPartitions()
    
        print '[',repeat-remaining+1, '/', repeat, ']',' in total partition', totalPartitions
    
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
