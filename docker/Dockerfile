FROM       mesosphere/mesos:1.0.0
MAINTAINER Yang Lei <yanglei@us.ibm.com>

# The generic Spark Mesos Docker image
#
# Folder structure:
#        Dockerfile
#        run.sh
#		 		the script that can start Spark Standalone Master/Slave, and also can submit spark job against all kinds of Spark Master
#
# To build image once:
#
#        docker build -t spark_mesosphere_mesos . 
#
#


# Installation Prereq

RUN apt-get update && apt-get install -y wget curl

# Download prebuild Spark 2.0.1 with Hadoop 2.7+

RUN wget http://d3kbcqa49mib13.cloudfront.net/spark-2.0.1-bin-hadoop2.7.tgz
RUN mkdir -p /usr/local
RUN tar -xvzf spark-2.0.1-bin-hadoop2.7.tgz -C /usr/local
ENV SPARK_HOME /usr/local/spark-2.0.1-bin-hadoop2.7
ENV PATH $SPARK_HOME/bin:$PATH

# Install JQ for processing JSON when needed

RUN apt-get install -y jq

# Install pip

RUN apt-get -y install python-pip
RUN pip -V

# Install the latest OpenJDK.
RUN \
    apt-get install -y software-properties-common;\
    add-apt-repository ppa:openjdk-r/ppa;\
    apt-get update;\
    apt-get install -y openjdk-8-jdk
    
ENV MESOS_NATIVE_JAVA_LIBRARY /usr/local/lib/libmesos.so
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

# Add run script

WORKDIR /spark

ADD run.sh /spark/
RUN chmod +x run.sh

VOLUME /spark/job

WORKDIR /spark/job

ENV MARATHON_HOST http://marathon.mesos:8080
ENV SPARK_PROCESS_NAME application

ENV SPARK_MASTER mesos://zk://leader.mesos:2181/mesos
ENV SPARK_MESOS_COARSE true

#ENV SPARK_ADDITIONAL_CONFIG --conf spark.mesos.executor.docker.image=yanglei99/spark_meso
#ENV SPARK_ADDITIONAL_JARS --packages --jars
#ENV SPARK_JOB CloudantDF.py

#ENV SPARK_MASTER_ID spark/master
#ENV SPARK_MASTER_HOST

CMD ["/spark/run.sh"]

