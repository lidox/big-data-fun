package reactiontest.compare;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
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
		
		// Pediction 1: avg off all reaction data + last 5 online (thumbeling window)
		HumanBenchmark human = new HumanBenchmark();
		human.loadDataSetOfOctober2016();
		double average = human.getAverageReaction();
		System.out.println("AVG:"+average);
		
		ReactionTestStream stream = new ReactionTestStream();
		//START KAFKA Broker and Zookeeper
		stream.getKafkaStream(); // data = .. and return
		stream.printAverage(Time.minutes(10));
		stream.printPredictionFOrNextReactionTimeByAVGs(average, Time.seconds(10));
		stream.execute();
		
		
		
		
		
		
		// Get Kafka Stream and sink it to elasticsearch
		//ReactionTestStream stream = new ReactionTestStream();
		
		//DataStream<String> kafkaStream = stream.getKafkaStream(); 
		//stream.sinkToElasticSearch(); // sink kafkaStream to elastic
		
		// combine streams
		//DataStream<String> esStream = stream.getElasticSearchStream();
		
		//ConnectedStreams<String, String> connectedStreams = kafkaStream.connect(esStream);

		

	}

}
