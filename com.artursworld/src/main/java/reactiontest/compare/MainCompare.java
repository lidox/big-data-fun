package reactiontest.compare;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;

import reactiontest.online.ReactionTestStream;

/**
 * Compare the online statistics with the offline computed statistics 
 * (integrate in the online analysis the offline values) 
 *
 */
public class MainCompare {

	public static void main(String[] args) throws Exception {
		// Get Kafka Stream and sink it to elasticsearch
		ReactionTestStream stream = new ReactionTestStream();
		
		DataStream<String> kafkaStream = stream.getKafkaStream(); 
		stream.sinkToElasticSearch(); // sink kafkaStream to elastic
		
		// combine streams
		DataStream<String> esStream = stream.getElasticSearchStream();
		
		ConnectedStreams<String, String> connectedStreams = kafkaStream.connect(esStream);

		

	}

}
