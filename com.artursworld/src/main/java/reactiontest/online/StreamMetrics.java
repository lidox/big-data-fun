package reactiontest.online;

import java.util.Date;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamMetrics {

	public static void main(String[] args) {
		
		try {
		
			// Condition for following metrics:
			// 1. running Apache Kafka
			// 2. insert data to Kafka by copy/paste following JSON string:
			//{ "medicalid":"Artur", "operationissue":"no-op", "age":24, "gender":"Male", "datetime":"2016-11-03 20:59:28.807", "type":"PreOperation", "times":[300,200,400,100] }
			
			StreamFunctions stream = new StreamFunctions();
			DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> dataStream1 = stream.getKafkaStream();
	
			// metric: count by tumbling window
			//stream.printCount(dataStream1, Time.seconds(10));
			
			// metric: average by tumbling window
			//stream.printAverage(dataStream1, Time.seconds(10));
			
			// metric: median by tumbling window
			//stream.printMedianByTimeWindow(dataStream1, Time.seconds(10));
			
			// metric: maximum by tumbling window
			//stream.printMinMaxByTimeWindow(dataStream1, Time.seconds(1), Integer.MAX_VALUE);
			
			// metric: minimum by tumbling window
			//stream.printMinMaxByTimeWindow(dataStream1, Time.seconds(1), Integer.MIN_VALUE);
			
			// metric: count by sliding window
			//stream.printCount(dataStream1, Time.seconds(30), Time.seconds(2));
			
			// print and execute
			stream.execute();
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
