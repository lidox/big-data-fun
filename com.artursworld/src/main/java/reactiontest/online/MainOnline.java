package reactiontest.online;

import org.apache.flink.streaming.api.windowing.time.Time;

public class MainOnline {

	public static void main(String[] args) throws Exception {
		ReactionTestStream stream = new ReactionTestStream();
		stream.initKafkaConsumer();
		
		// metric: count by tumbling window
		stream.printCount(Time.seconds(10));
		
		
		
		// print and execute
		//stream.print();
		stream.execute();
	}

}
