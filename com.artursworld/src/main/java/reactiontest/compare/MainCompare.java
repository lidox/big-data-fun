package reactiontest.compare;

import org.apache.flink.streaming.api.windowing.time.Time;

import reactiontest.offline.HumanBenchmark;
import reactiontest.online.ReactionTestStream;

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
		//printPredictionByAverage();
		
		// Prediction 2:
		//printPredictionByAVGofMedians();
		
		// Prediction 3:
		printPredictionBySlidingAverage();
		
	}

	private static void printPredictionByAVGofMedians() throws Exception {
		// Prediction 2: Median off all reaction data since October 2016 
		// and last reaction times specified by tumbling window
		HumanBenchmark human = new HumanBenchmark();
		human.loadDataSetOfOctober2016();
		double median = human.getMedianReactionTime();
		
		
		ReactionTestStream stream = new ReactionTestStream();
		stream.getKafkaStream();
		stream.printPredictionForNextReactionTimeByMedians(median, Time.seconds(10));
		stream.execute();
	}

	private static void printPredictionByAverage() throws Exception {
		// Prediction 1: Average off October 2016 reaction data + tumbling window
		HumanBenchmark human = new HumanBenchmark();
		human.loadDataSetOfOctober2016();
		double average = human.getAverageReaction();
		
		
		ReactionTestStream stream = new ReactionTestStream();
		//START KAFKA Broker and Zookeeper
		stream.getKafkaStream();
		stream.printPredictionForNextReactionTimeByAVGs(average, Time.seconds(10));
		stream.execute();
	}
	
	private static void printPredictionBySlidingAverage() throws Exception {
		// Prediction 1: Average off October 2016 reaction data + tumbling window
		HumanBenchmark human = new HumanBenchmark();
		human.loadDataSetOfOctober2016();
		double average = human.getAverageReaction();
		
		
		ReactionTestStream stream = new ReactionTestStream();
		//START KAFKA Broker and Zookeeper
		stream.getKafkaStream();
		stream.printPredictionForNextReactionTimeBySlidingAVGs(average, Time.seconds(10), Time.seconds(3));
		stream.execute();
	}

}
