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

## Elastic Search 
How to [setup](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html) elastic search. Try [Ubuntu installation guide] (https://www.elastic.co/guide/en/elasticsearch/reference/current/deb.html). 

[ElasticSearch Installation 2.3.2](https://www.elastic.co/guide/en/elasticsearch/reference/2.3/_installation.html)

How to use the [Flink elasticsearch connector](https://ci.apache.org/projects/flink/flink-docs-master/dev/connectors/elasticsearch.html)

[ElasticSearch Flink and Kafka](https://github.com/keiraqz/KafkaFlinkElastic)

### Configure Elasticsearch
First create an index:
```
# create reactiontest index
curl -XPUT 'http://localhost:9200/reactiontest/' -d '{
    "settings" : {
        "index" : {
            "number_of_shards" : 1, 
            "number_of_replicas" : 0
        }
    }
}'
```
Make sure you created the index by following message: {"acknowledged":true}

Now create a mapping
```

```

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

## Start Kafka
```
cd /opt/kafka_2.10-0.8.2.1

# start zookeeper server
./bin/zookeeper-server-start.sh ./config/zookeeper.properties

# start broker
./bin/kafka-server-start.sh ./config/server.properties 

# create topic “test”
 ./bin/kafka-topics.sh --create --topic reactiontest --zookeeper localhost:2181 --partitions 1 --replication-factor 1

# consume from the topic using the console producer
./bin/kafka-console-consumer.sh --topic reactiontest --zookeeper localhost:2181

# produce something into the topic (write something and hit enter)
./bin/kafka-console-producer.sh --topic reactiontest --broker-list localhost:9092
```
## Start ElasticSearch
```
cd /Dokumente/elasticsearch-2.3.4/bin

# start elasticsearch
./elasticsearch --cluster.name my-demo --node.name my-node

# make a search
curl 'localhost:9200/viper-test/viper-log/_search?regina'

```
