package de.heinzen.christoph.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;

/**
 * Flink Job for the following exercise:
 * 
 * http://dataartisans.github.io/flink-training/exercises/rideCleansing.html
 *
 */
public class PopularPlacesJob {

	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		
		final int valueToBeAPopularPlace = 10; 
		
		// get an ExecutionEnvironment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// configure event-time processing
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// get the taxi ride data stream
		DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource("src/main/nycTaxiRides.gz", 5, 5));
		
		//filter the rides in the NYC-area
		SingleOutputStreamOperator<TaxiRide> nycRides = rides.filter(new NYCAreaFilter());
		
		SingleOutputStreamOperator<Tuple5<Float, Float, Long, Boolean, Integer>> ridesCountByArea = nycRides
		.flatMap(new FlatMapFunction<TaxiRide, Tuple2<Integer, Boolean>>() {
			@Override
			public void flatMap(TaxiRide value, Collector<Tuple2<Integer, Boolean>> out) throws Exception {
				out.collect(new Tuple2<>(GeoUtils.mapToGridCell(value.startLon, value.startLat), true));
				out.collect(new Tuple2<>(GeoUtils.mapToGridCell(value.endLon, value.endLat), false));
			}
		})
		.keyBy(0,1)
		.timeWindow(Time.minutes(15), Time.minutes(5))
		.apply(new WindowFunction<Tuple2<Integer,Boolean>, Tuple5<Float, Float, Long, Boolean, Integer>, Tuple, TimeWindow>() {

			@SuppressWarnings("unchecked")
			@Override
			public void apply(Tuple key, TimeWindow window,
					Iterable<Tuple2<Integer, Boolean>> input,
					Collector<Tuple5<Float, Float, Long, Boolean, Integer>> out)
					throws Exception {

				boolean isStart = ((Tuple2<Integer, Boolean>)key).f1;
				int gridCell = ((Tuple2<Integer, Boolean>)key).f0;
				float lon = GeoUtils.getGridCellCenterLon(gridCell);
				float lat = GeoUtils.getGridCellCenterLat(gridCell);
				int count = 0;
				
				for (Tuple2<Integer, Boolean> tuple : input) {
					count++;
				}
				
				if (count > valueToBeAPopularPlace) {
					out.collect(new Tuple5<>(lon, lat, window.getEnd(), isStart, count));
				}
				
			}
		});
		
		ridesCountByArea.print();
		
		
		env.execute("Popular Places Job");
		
	}
	
}
