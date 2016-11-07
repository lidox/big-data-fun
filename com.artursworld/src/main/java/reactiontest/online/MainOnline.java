package reactiontest.online;

import org.apache.flink.streaming.api.windowing.time.Time;

public class MainOnline {

	public static void main(String[] args) throws Exception {
		//runOfflineAnalysis();
		
		ReactionTestStream stream = new ReactionTestStream();
		stream.sink();
		
	}

	public static void runOfflineAnalysis() throws Exception {
		ReactionTestStream stream = new ReactionTestStream();
		stream.initKafkaConsumer();
		
		//This is a valid request to kafka:
		//{ "medicalid":"Artur", "operationissue":"no-op", "age":24, "gender":"Male", "datetime":"2016-11-03 20:59:28.807", "type":"PreOperation", "times":[300,200,400,100] }
		
		// metric: count by tumbling window
		stream.printCount(Time.seconds(10));
		
		// metric: average by tumbling window
		stream.printAverage(Time.seconds(10));
		
		// metric: median by tumbling window
		stream.printMedianByTimeWindow(Time.seconds(10));
		
		// metric: maximum by tumbling window
		stream.printMinMaxByTimeWindow(Time.seconds(1), Integer.MAX_VALUE);
		
		// metric: minimum by tumbling window
		stream.printMinMaxByTimeWindow(Time.seconds(1), Integer.MIN_VALUE);
		
		// print and execute
		//stream.print();
		stream.execute();
	}

}
