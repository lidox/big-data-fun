package reactiontest.online;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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

	/**
	 * Sets the time window
	 * @param time the time window to set
	 */
	public void setTimeWindow(Time time){
		TIME_WINDOW = time;
	}
	
	/**
	 * Metric Count: Calculate the reaction test count passed through the window
	 * @param data
	 * @return
	 */
	public SingleOutputStreamOperator<Tuple1<Integer>> getCount(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data) {
		return data.keyBy(0)
				.timeWindow(TIME_WINDOW)
				.apply(new Counter());
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
	
	
	public SingleOutputStreamOperator<Tuple2<Double, String>> getAverageReactionTime(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data) {
		return data.keyBy(4) // group by timeStamp
				.timeWindow(TIME_WINDOW)
				.apply(new AverageWindowFunction());
	}
	
	/**
	 * Calculates the average reaction time in the specified window
	 * Returns a tuple<avg, timeStamp>
	 */
	public static class AverageWindowFunction implements WindowFunction<Tuple7<String, String, Integer, String, Date, String, List<Double>> 
	, Tuple2<Double, String>, Tuple, TimeWindow>{

		private static final long serialVersionUID = 968401493677797810L;

		@Override
		public void apply(
				Tuple key,
				TimeWindow window,
				Iterable<Tuple7<String, String, Integer, String, Date, String, List<Double>>> input,
				Collector<Tuple2<Double, String>> out) throws Exception {
			
			// collect reaction times into list
			List<Double> reactionTimeList = new ArrayList<Double>();
			for(Tuple7<String, String, Integer, String, Date, String, List<Double>> tuple: input){
				reactionTimeList.addAll(tuple.f6);
			}
			
			// calculate reaction time count
			double reactionTimeCount = reactionTimeList.size();
			
			// calculate reaction time 
			double reactioTimeSum = 0;
			for(Double reactionTime: reactionTimeList){
				reactioTimeSum += reactionTime;
			}
						
			double avg = 0;
			
			if(reactionTimeCount != 0)
				avg = reactioTimeSum / reactionTimeCount;
			
			out.collect(new Tuple2<Double, String>(avg, new Date().toString()));
		}
		
	}
	
	public SingleOutputStreamOperator<Tuple3<Double, String, Integer>> getMedianReactionTime(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data) {
		return data.keyBy(4) // group by timeStamp
				.timeWindow(TIME_WINDOW)
				.apply(new MedianWindowFunction());
	}
	
	/**
	 * Calculates the median reaction time in the specified window
	 */
	public static class MedianWindowFunction implements WindowFunction<Tuple7<String, String, Integer, String, Date, String, List<Double>> 
	, Tuple3<Double, String, Integer>, Tuple, TimeWindow>{

		private static final long serialVersionUID = 968401493677797810L;

		@Override
		public void apply(
				Tuple key,
				TimeWindow window,
				Iterable<Tuple7<String, String, Integer, String, Date, String, List<Double>>> input,
				Collector<Tuple3<Double, String, Integer>> out) throws Exception {
			
			// collect reaction times into list
			List<Double> reactionTimeList = new ArrayList<Double>();
			for(Tuple7<String, String, Integer, String, Date, String, List<Double>> tuple: input){
				reactionTimeList.addAll(tuple.f6);
			}
			
			// sort the list
			Collections.sort(reactionTimeList);
			
			// calculate reaction time count
			int reactionTimeCount = reactionTimeList.size();

		    // calculate median
			double median = 0;
		    if (reactionTimeCount % 2 == 0)
		        median = (reactionTimeList.get(reactionTimeCount/2) + reactionTimeList.get(reactionTimeCount / 2 - 1))/2;
		    else
		        median = (double) reactionTimeList.get(reactionTimeCount/2);
			
		    // Tuple3 = median, timeStamp, test count
			out.collect(new Tuple3<Double, String, Integer>(median, new Date().toString(), reactionTimeCount));
		}
		
	}
	
	public SingleOutputStreamOperator<Tuple3<Double, String, Integer>> getMinMaxReactionTimeByTimeWindow(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data, int value) {
		return data.keyBy(4) // group by timeStamp
				.timeWindow(TIME_WINDOW)
				.apply(new MinMaxWindowFunction(value));
	}
	
	/**
	 * Calculates the min / max reaction time in the specified window
	 */
	public static class MinMaxWindowFunction implements WindowFunction<Tuple7<String, String, Integer, String, Date, String, List<Double>> 
	, Tuple3<Double, String, Integer>, Tuple, TimeWindow>{

		private static final long serialVersionUID = 968401493677797810L;

		private boolean isMaximum = false;
		
		public MinMaxWindowFunction(int value) {
			isMaximum = (value == Integer.MAX_VALUE);
		}

		@Override
		public void apply(
				Tuple key,
				TimeWindow window,
				Iterable<Tuple7<String, String, Integer, String, Date, String, List<Double>>> input,
				Collector<Tuple3<Double, String, Integer>> out) throws Exception {
			
			// collect reaction times into list
			List<Double> reactionTimeList = new ArrayList<Double>();
			for(Tuple7<String, String, Integer, String, Date, String, List<Double>> tuple: input){
				reactionTimeList.addAll(tuple.f6);
			}
			
			int index = 0;
			if(isMaximum)
				index = reactionTimeList.indexOf(Collections.max(reactionTimeList));
			else
				index = reactionTimeList.indexOf(Collections.min(reactionTimeList));
			
		    // Tuple3 = minimum/maximum ,timeStamp, test count
			out.collect(new Tuple3<Double, String, Integer>(reactionTimeList.get(index), new Date().toString(), reactionTimeList.size()));
		}
		
	}

 
	/**
	 * Make prediction
	 * @param data
	 * @param avg
	 * @return
	 */
	public SingleOutputStreamOperator<Tuple2<String, Double>> getPredictedReactionTimeByAVGs(
			DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data, double avg) {
		    return data.keyBy(4) // group by timeStamp
				.timeWindow(TIME_WINDOW)
				.apply(new AverageWindowFunction()) 
				.keyBy(0)
				.flatMap(new PedictionByAVGFlatMap(avg));
	}
	
	/**
	 * 
	 * Return tuple<message, prediction>
	 *
	 */
	public static class PedictionByAVGFlatMap implements FlatMapFunction<Tuple2<Double, String>, Tuple2<String, Double>>{

		private static final long serialVersionUID = 71246547222383551L;
		private  double offlineAVG = 0;
		
		PedictionByAVGFlatMap(double avg){
			offlineAVG = avg;
		}

		@Override
		public void flatMap(Tuple2<Double, String> in, Collector<Tuple2<String, Double>> out) {
			double onlineAVG = in.f0;
			double prediction = (offlineAVG + onlineAVG) / 2; 			
			out.collect(new Tuple2<String, Double>("Prediction by AVG.  Next RT will be",prediction));
		}
		
	}


	public SingleOutputStreamOperator<Tuple2<String, Double>> getPredictedReactionTimeByMedians(
			DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data,
			double median) {
	    return data.keyBy(4) 
			.timeWindow(TIME_WINDOW)
			.apply(new MedianWindowFunction()) 
			.keyBy(0)
			.flatMap(new PedictionByMedianFlatMap(median));
	}
	
	/**
	 * AVG of Medians
	 * Return tuple<message, prediction>
	 *
	 */
	public static class PedictionByMedianFlatMap implements FlatMapFunction<Tuple3<Double, String, Integer>, Tuple2<String, Double>>{

		private static final long serialVersionUID = 72346547222383551L;
		private  double offlineMedian = 0;
		
		PedictionByMedianFlatMap(double offlineMedian){
			this.offlineMedian = offlineMedian;
		}

		@Override
		public void flatMap(Tuple3<Double, String, Integer> in, Collector<Tuple2<String, Double>> out) {
			double onlineMedian = in.f0;
			double prediction = (offlineMedian + onlineMedian) / 2; 			
			out.collect(new Tuple2<String,Double>("Prediction by AVG of Median. NEXT RT:", prediction)); 
		}
		
	}
	
}