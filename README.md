# Big Data Fun

## Prepare and Installation Guides
See following [instructions ](https://gist.github.com/lidox/ae71fe107792534cc13cd887175dede4) to prepare our tools and data

## Get familiar with Apache Flink

[Following guide](http://dataartisans.github.io/flink-training/) is a good beginning!

## Get familiar with Apache Kafka

[Kafka example project](https://github.com/dataArtisans/kafka-example) on Github

Combine [Kafka with Flink](http://data-artisans.com/kafka-flink-a-practical-how-to) or better check [this cool blog post](https://www.javacodegeeks.com/2016/10/getting-started-apache-flink-kafka.html) to do so!

To get Kafka to work on Android -> maybe try to build an apk via Maven 
and add all dependecies from already working kafka-flink client (pom.xml) to android project.

See folling [blog article](http://www.vogella.com/tutorials/AndroidBuildMaven/article.html)


## Get familiar with Hadoop 2.6.0
Download:
https://archive.apache.org/dist/hadoop/core/hadoop-2.6.0/

## Install:
1. [Best guide](http://www.bogotobogo.com/Hadoop/BigData_hadoop_Install_on_ubuntu_single_node_cluster.php)
2. [Good guide](http://pingax.com/install-hadoop2-6-0-on-ubuntu)

## Connect Kafka with HDFS
https://www.youtube.com/watch?v=imDtlYXpRgc



# Demo
## Run Hadoop 2.6.0
```
cd /usr/local/hadoop/sbin

sudo su hduser

start-all.sh
```

## Monitoring Hadoop
[Web UI of the NameNode daemon](http://localhost:50070)

[Logs](http://localhost:50070/logs/)


