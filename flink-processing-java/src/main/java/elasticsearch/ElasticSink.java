package elasticsearch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;

public class ElasticSink {

	
	private StreamExecutionEnvironment env = null;
	private String csvPath = null;
	private final int maxEventDelay = 60;       // events are out of order by max 60 seconds
	private final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second
	private DataStream<TaxiRide> dataStream; 
	
	public ElasticSink(String path) {
		this.env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		this.csvPath = path;
		
		// configure event-time processing
		this.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		// get the taxi ride data stream
		dataStream = env.addSource(new TaxiRideSource(this.csvPath, maxEventDelay, servingSpeedFactor));
	}
	
	/**
	 * Prints the DataStream to console
	 */
	public void print(){
		if(dataStream != null)
			try {
				dataStream.print();
				env.execute("New Yorker Taxi Rides");
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
	
	

}
