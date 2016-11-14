package stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;

public class TaxiStreamProcessor {
	
	private StreamExecutionEnvironment env = null;
	private String csvPath = null;
	final int maxEventDelay = 60;       // events are out of order by max 60 seconds
	final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second
	
	public TaxiStreamProcessor(String path) {
		this.env = StreamExecutionEnvironment.getExecutionEnvironment();
		this.csvPath = path;
	}
	
	
	public void test() throws Exception{
		// configure event-time processing
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		/**
		   In order to get dependencies please do:
		   1. First 
		   git clone https://github.com/dataArtisans/flink-training-exercises.git
			cd flink-training-exercises
			mvn clean install
			
			2. Second: add dependencies
				<!-- training libraries --> 
				<dependency>
				  <groupId>com.dataartisans</groupId>
				  <artifactId>flink-training-exercises</artifactId>
				  <version>0.5</version>
				</dependency>
		 */
		
		// get the taxi ride data stream
		DataStream<TaxiRide> rides = 
				env.addSource(new TaxiRideSource(this.csvPath, maxEventDelay, servingSpeedFactor));
		
		DataStream<TaxiRide> filterRides = rides.filter(new FilterFunction<TaxiRide>() {
			
			@Override
			public boolean filter(TaxiRide singleRide) throws Exception {
				boolean isNewYorkerTaxiDude = false;
				isNewYorkerTaxiDude = GeoUtils.isInNYC(singleRide.endLon, singleRide.endLat);
				return isNewYorkerTaxiDude;
			}
		});
		
		filterRides.print();
		
		env.execute("New Yorker Taxi Rides");
	}

}
