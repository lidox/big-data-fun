package reactiontest.online;

import java.util.Date;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Provides some metric functions for online analysis
 *
 */
public class OnlineMetrics {
	
	private Time TIME_WINDOW = Time.seconds(10);

	public SingleOutputStreamOperator<Tuple1<Integer>> getCount(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data) {
		return data.keyBy(0)
				.timeWindow(TIME_WINDOW)
				.apply(new Counter());
	}
	
	public void setTimeWindow(Time time){
		TIME_WINDOW = time;
	}

	/**
	 * Counts the elements in the window
	 *
	 */
	public static class Counter implements WindowFunction<Tuple7<String, String, Integer, String, Date, String, List<Double>> 
	, Tuple1<Integer>, Tuple, TimeWindow>{

		private static final long serialVersionUID = 968401493677797810L;

		@Override
		public void apply(
				Tuple key,
				TimeWindow window,
				Iterable<Tuple7<String, String, Integer, String, Date, String, List<Double>>> input,
				Collector<Tuple1<Integer>> out) throws Exception {
			
			
			//Date timeStamp = ((Tuple1<Date>) key).f0;
			int counter = 0;
		
			for(Tuple7<String, String, Integer, String, Date, String, List<Double>> tuple: input){
				counter ++;
			}
			
			out.collect(new Tuple1<Integer>(counter));
		}
		
	}
	
	// count, minimum, maximum, avg, median

}
