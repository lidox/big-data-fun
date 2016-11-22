package reactiontest.compare;

import java.util.Date;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import reactiontest.offline.BatchFunctions;
import reactiontest.online.StreamFunctions;

/**
 * Compare the online statistics with the offline computed statistics 
 * (integrate in the online analysis the offline values) 
 *
 */
public class MainCompare {

	public static void main(String[] args) throws Exception {
		
		// Condition for following metrics:
		// 1. running Apache Kafka
		// 2. insert data to Kafka by copy/paste following JSON string:
		// {"medicalid":"Fabio", "operationissue":"italia", "age":33, "gender":"Male", "datetime":"2016-11-03 20:59:28.807", "type":"PreOperation", "times":[141,1750,2000] }
		
		
		// Prediction 1:
		printPredictionByAverage();
		
		// Prediction 2:
		printPredictionByAVGofMedians();
		
		// Prediction 3:
		printPredictionBySlidingAverage();
		
	}

	private static void printPredictionByAVGofMedians() throws Exception {
		// Prediction 2: Median off all reaction data since October 2016 
		// and last reaction times specified by tumbling window
		BatchFunctions batch = new BatchFunctions();
		DataSet<Tuple2<Double, Integer>> dataSet1 = batch.loadDataSetOfOctober2016();
		double median = batch.getMedianReactionTime(dataSet1);
		
		
		StreamFunctions stream = new StreamFunctions();
		DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> dataStream1 = stream.getKafkaStream();
		stream.printPredictionForNextReactionTimeByMedians(dataStream1, median, Time.seconds(10));
		stream.execute();
	}

	private static void printPredictionByAverage() throws Exception {
		// Prediction 1: Average off October 2016 reaction data + tumbling window
		BatchFunctions human = new BatchFunctions();
		DataSet<Tuple2<Double, Integer>> dataSet1 = human.loadDataSetOfOctober2016();
		double average = human.getAverageReaction(dataSet1);
		
		
		StreamFunctions stream = new StreamFunctions();
		//START KAFKA Broker and Zookeeper
		DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> dataStream1 = stream.getKafkaStream();
		stream.printPredictionForNextReactionTimeByAVGs(dataStream1, average, Time.seconds(10));
		stream.execute();
	}
	
	private static void printPredictionBySlidingAverage() throws Exception {
		// Prediction 1: Average off October 2016 reaction data + tumbling window
		BatchFunctions human = new BatchFunctions();
		DataSet<Tuple2<Double, Integer>> dataSet1 = human.loadDataSetOfOctober2016();
		double average = human.getAverageReaction(dataSet1);
		
		
		StreamFunctions stream = new StreamFunctions();
		DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> dataStream1 = stream.getKafkaStream();
		stream.printPredictionForNextReactionTimeBySlidingAVGs(dataStream1, average, Time.seconds(10), Time.seconds(3));
		stream.execute();
	}

}
