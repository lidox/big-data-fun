# Pre Submission Details

## 1. Data connection and data collection
I want to read reation time data from [Human Benchmark](http://www.humanbenchmark.com/tests/reactiontime/statistics/)
and collect data using a self created data stream via Apache Kafka.

## 2. Create the data collection for online and historic analysis 
Connected to [Human Benchmark](http://www.humanbenchmark.com/tests/reactiontime/statistics/).
Data Collecetion via own Kafka-Stream.

## 3. Create the stream (online) metric computation implementation
The Class 'MainOnline' contains 5 metric functions like:
```java
// metric: count by tumbling window
stream.printCount(Time.seconds(10));
		
// metric: average by tumbling window
stream.printAverage(Time.seconds(10));
		
// metric: median by tumbling window
stream.printMedianByTimeWindow(Time.seconds(10));

// so on...
```

## 4. Create the batch(historic) metric computation implementation
The Class 'MainOffline' contains metric functions for offline analyis:
```java
// read data using flatMap function
HumanBenchmark human = new HumanBenchmark();
human.loadDataSetOfOctober2016();

// metric: count using flinks sum function
System.err.println("1. RT Count: "+human.getReactionTestCountBySum()); 
		
// metric: count using flinks reduce function
System.err.println("2. RT Count: "+human.getReactionTestCountByReduce()); 
		
// metric: minimum 
Tuple2<Double, Integer> recordWithMinUserCount = human.getReactionTimeByMinUserCount();
System.err.println("Min user count = "+recordWithMinUserCount.f1+" with RT = "+recordWithMinUserCount.f0);
		
// so on...
```
## 5. Compare the online statistics with the offline computed statistics (integrate in the online analysis the offline values)
The Class 'MainCompare' integrates in the online analysis the offline values
```java
// Prediction 1: Average off October 2016 reaction data + tumbling window
HumanBenchmark human = new HumanBenchmark();
human.loadDataSetOfOctober2016();
double average = human.getAverageReaction(); // get offline value
		
		
ReactionTestStream stream = new ReactionTestStream();
stream.getKafkaStream();
stream.printPredictionFOrNextReactionTimeByAVGs(average, Time.seconds(10)); // integrate offline value
stream.execute();
// so on...
```
## 6. Study options for making predictions about 3 statistics (including using ML libraries or implementing own (simple) prediction algorithms) 
The Class 'MainCompare' predictions about 3 statistics:
```java
// Prediction 1:
printPredictionByAverage();
		
// Prediction 2:
printPredictionByAVGofMedians();

// so on...
```

## 7. Create a visualization and prepare the demo (GUI, batch, excel....) 
The data can be visualized via Eclipse Console and [Kibana](https://www.elastic.co/de/products/kibana).
In order to demonstrate the function Eclipse will be used.

## 8. Prepare the demo submission 
That's what this document stands for! :)

## 9. Solve various bugs that appear along the way - 3 hours (10% of the workload :) )
```java
// TODO: find bugs and fix them! :)
```




