package reactiontest.online;

import java.util.Date;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Provides some metric functions for online analysis
 *
 */
public class OnlineMetrics {

	public int getCount(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data) {
		data.keyBy(0)
				.timeWindow(Time.minutes(1))
				.apply(new CountByTimestamp())
				.timeWindowAll(Time.seconds(1)).sum(1).print();
		return 0;
	}

	public static class CountByTimestamp implements WindowFunction<Tuple7<String, String, Integer, String, Date, String, List<Double>> 
	, Tuple2<String, Integer>, Tuple, TimeWindow>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 968401493677797810L;

		@Override
		public void apply(
				Tuple key,
				TimeWindow window,
				Iterable<Tuple7<String, String, Integer, String, Date, String, List<Double>>> input,
				Collector<Tuple2<String, Integer>> out) throws Exception {
			
			
			Date timestamp = ((Tuple1<Date>) key).f0;
			int counter = 0;
			
			for(Tuple7<String, String, Integer, String, Date, String, List<Double>> item: input){
				counter ++;
			}
			
			out.collect(new Tuple2<>(""+timestamp, counter));
		}
		
	}
	// count, minimum, maximum, avg, median

}
