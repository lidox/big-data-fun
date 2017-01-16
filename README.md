# Big Data Fun

## Preparetion and Installation Guides
See following [instructions ](https://gist.github.com/lidox/ae71fe107792534cc13cd887175dede4) to prepare your machine installing MAVEN, Java etc.

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

#### Install:
1. [Best guide](http://www.bogotobogo.com/Hadoop/BigData_hadoop_Install_on_ubuntu_single_node_cluster.php)
2. [Good guide](http://pingax.com/install-hadoop2-6-0-on-ubuntu)

#### Connect Kafka with HDFS
https://www.youtube.com/watch?v=imDtlYXpRgc

#### Run Hadoop 2.6.0
```
cd /usr/local/hadoop/sbin

sudo su hduser

start-all.sh
```

#### Monitoring Hadoop
[Web UI of the NameNode daemon](http://localhost:50070)

[Logs](http://localhost:50070/logs/)

## Get familiar with Elastic Search 
How to [setup](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html) elastic search. Try [Ubuntu installation guide] (https://www.elastic.co/guide/en/elasticsearch/reference/current/deb.html). 

[ElasticSearch Installation 2.3.2](https://www.elastic.co/guide/en/elasticsearch/reference/2.3/_installation.html)

How to use the [Flink elasticsearch connector](https://ci.apache.org/projects/flink/flink-docs-master/dev/connectors/elasticsearch.html)

[ElasticSearch Flink and Kafka](https://github.com/keiraqz/KafkaFlinkElastic)

#### Configure Elasticsearch
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
# put mapping for viper-log doctype
curl -XPUT 'localhost:9200/reactiontest/_mapping/reactiontest-log' -d '{
      "properties": {
            "medicalid": {
              "type": "string",
              "index": "not_analyzed"
            },
            "operationissue": {
                "type": "string"
            },
            "age": {
               "type": "integer"
             },
            "gender": {
               "type": "string"
            },
            "datetime": {
               "type": "string"
            },
            "type": {
               "type": "string"
            },
            "times": {
                "type":"integer",
                "store":"yes"
            }
      }
}'
```

## Get familiar with Kibana

#### Install Kibana 4.5.1
First download [Kibana 4.5.1](https://www.elastic.co/downloads/past-releases/kibana-4-5-1). 
Otherwise use the terminal
```
wget "https://download.elastic.co/kibana/kibana/kibana_4.5.1_amd64.deb"
```
To install use:
```
sudo dpkg -i kibana_4.5.1_amd64.deb
```
# DEMO

## Start Kafka
```
cd /opt/kafka_2.10-0.8.2.1

# start zookeeper server
./bin/zookeeper-server-start.sh ./config/zookeeper.properties 

# start broker
./bin/kafka-server-start.sh ./config/server.properties 

# create topic “reactiontest”
 ./bin/kafka-topics.sh --create --topic reactiontest --zookeeper localhost:2181 --partitions 1 --replication-factor 1

# consume from the topic using the console producer
./bin/kafka-console-consumer.sh --topic reactiontest --zookeeper localhost:2181

# produce something into the topic (write something and hit enter)
./bin/kafka-console-producer.sh --topic reactiontest --broker-list localhost:9092
```
Producer message:
```
{"medicalid":"Markus", "operationissue":"foobar", "age":54, "gender":"Male", "datetime":"2016-11-03 20:59:28.807", "type":"PreOperation", "times":[412,399,324] }
```
## Start ElasticSearch
```
cd ~/Dokumente/elasticsearch-2.3.4/bin

# start elasticsearch
./elasticsearch --cluster.name my-demo --node.name my-node &

# make a search
curl 'localhost:9200/viper-test/viper-log/_search?regina'

```

## Start Eclipse IDE
```
cd /opt/eclipse/
./eclipse &
```
## Start Kibana
```
cd /opt/kibana/bin
sudo ./kibana
```
Now check [the browser](http://localhost:5601)

# Additional stuff
To make things easier a start script could be created for
all tools used like ElasticSearch, Kibana, Kafka etc.
```
sudo nano /usr/local/bin/bigdata

#!/bin/sh
/home/lidox/Dokumente/elasticsearch-2.3.4/bin/elasticsearch --cluster.name my-demo --node.name my-node &

/opt/kafka_2.10-0.8.2.1/bin/zookeeper-server-start.sh ./config/zookeeper.properties &

/opt/kafka_2.10-0.8.2.1/bin/kafka-server-start.sh ./config/server.properties &

sudo chmod +x /usr/local/bin/bigdata
sudo netstat -nlp

```
