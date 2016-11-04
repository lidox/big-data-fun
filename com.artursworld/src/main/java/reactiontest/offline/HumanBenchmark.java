package reactiontest.offline;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONArray;

/**
 * Offline data analysis via Flink. The data is comming from 
 * www.humanbenchmark.com/tests/reactiontime
 */
public class HumanBenchmark {
	
	// configuration
	private String filePathJSON = "human-benchmark-october.json";
	
	private ExecutionEnvironment env = null;
	DataSet<Tuple2<Double, Integer>> data = null;
	
	public HumanBenchmark() {
		try {		
			env = ExecutionEnvironment.getExecutionEnvironment();
		} catch(Exception e) {
			  e.printStackTrace();
		}
	}
	
	public void loadDataSetOfAllTime(){
		DataSet<String> inputData = env.readTextFile(getFilePath("human-benchmark-alltime.json"));
		data = inputData.flatMap(new Tokenizer());
	}
	
	public void loadDataSetOfOctober2016(){
		DataSet<String> inputData = env.readTextFile(getFilePath(filePathJSON));
		data = inputData.flatMap(new Tokenizer());
	}
	
	public void printInputData(){
		if(data != null)
			try {
				data.print();
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
	
	/**
	 * Get reaction test count by sum function
	 * @return the count of all reaction tests
	 */
	public int getReactionTestCountBySum(){
		int count = 0;
		
		try {
			DataSet<Tuple2<Double, Integer>> sumData = data.sum(1);
			count = sumData.collect().get(0).f1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return count;
	}
	
	/**
	 * Get reaction test count by reduce function
	 * @return the count of all reaction tests
	 */
	public int getReactionTestCountByReduce(){
		int count = 0;
		
		try {
			DataSet<Tuple2<Double, Integer>> sumData = data.reduce(new ReduceFunction<Tuple2<Double,Integer>>() {

				private static final long serialVersionUID = -5937101140633725165L;

				@Override
				public Tuple2<Double, Integer> reduce(Tuple2<Double, Integer> recent,Tuple2<Double, Integer> current) {
					Tuple2<Double, Integer> ret = new Tuple2<Double, Integer>(0.,0);
					try {
						ret = new Tuple2<Double, Integer>(current.f0, recent.f1 + current.f1);
					} catch (Exception e) {
						e.printStackTrace();
					}
					return ret;
				}
			});
			count = sumData.collect().get(0).f1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return count;
	}

	/**
	 * Get the file path from resource folder by filename
	 * @param fileName the name of the file
	 * @return the absolute file path
	 */
	public String getFilePath(String fileName) {
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource(fileName).getFile());
		String fileToRead = file.getAbsolutePath();
		return fileToRead;
	}
	
	public static class Tokenizer implements FlatMapFunction<String, Tuple2<Double, Integer>>{

		private static final long serialVersionUID = 8139075182383902551L;

		@Override
		public void flatMap(String value, Collector<Tuple2<Double, Integer>> out) {
			// get string of data#
			List<Tuple2<Double, Integer>> jsonList = getListByJSON(value);
			
			for(Tuple2<Double, Integer> item: jsonList){
				out.collect(item);
			}
		}
		
		public static List<Tuple2<Double, Integer>> getListByJSON(String jsonString){    
			List<Tuple2<Double, Integer>> retList = new ArrayList<>();
			try {
					JSONArray transactionJSON = new JSONArray(jsonString);
					for(int i = 0 ; i< transactionJSON.length(); i++){
							JSONArray a = (JSONArray) transactionJSON.get(i);
							Double reactionTime = Double.parseDouble(a.get(0).toString());
							Integer userCount = Integer.parseInt(a.get(1).toString());
							retList.add(new Tuple2<Double, Integer>(reactionTime, userCount));
					}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			return retList;
		}
	}

	/**
	 * Get the reaction time with the minimal user count using the reduce function
	 * @return the reaction time tuple with the minimal user count
	 */
	public Tuple2<Double, Integer> getReactionTimeByMinUserCount() {
		Tuple2<Double, Integer> ret = new Tuple2<Double, Integer>();
		try {
			DataSet<Tuple2<Double, Integer>> minData = data.reduce(new ReduceFunction<Tuple2<Double,Integer>>() {
			
				private static final long serialVersionUID = 1439796543225086584L;
	
				@Override
				public Tuple2<Double, Integer> reduce(Tuple2<Double, Integer> recent, Tuple2<Double, Integer> current) {
					int recentUserCount = recent.f1;		
					int currentUserCount = current.f1;
					
					if(recentUserCount < currentUserCount){
						return recent;
					} 
					else{
						return current;
					}
				}
			});
		
			ret = minData.collect().get(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

	/**
	 * Get the reaction time with the maximal user count using the reduce function
	 * @return the reaction time tuple with the maximal user count
	 */
	public Tuple2<Double, Integer> getReactionTimeByMaxUserCount() {
		Tuple2<Double, Integer> ret = new Tuple2<Double, Integer>();
		try {
			DataSet<Tuple2<Double, Integer>> maxData = data.reduce(new ReduceFunction<Tuple2<Double,Integer>>() { 
			
				private static final long serialVersionUID = 1439796543225086584L;
	
				@Override
				public Tuple2<Double, Integer> reduce(Tuple2<Double, Integer> recent, Tuple2<Double, Integer> current) {
					int recentUserCount = recent.f1;		
					int currentUserCount = current.f1;
					
					if(recentUserCount > currentUserCount){
						return recent;
					} 
					else{
						return current;
					}
				}
			});
		
			ret = maxData.collect().get(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

	/**
	 * Get the max user count by build in aggregation functions
	 * @return a tuple of max reaction time and user count for a specific reaction test
	 */
	public Tuple2<Double, Integer> getMaxUserCountByAggregate() {
		Tuple2<Double, Integer> ret = null;
		DataSet<Tuple2<Double, Integer>> output = data
                .max(0)
                .andMax(1);
		try {
			ret = output.collect().get(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

	/**
	 * computes the average reaction time of the users
	 * @return the average reaction time 
	 * @throws Exception
	 */
	public double getAverageReaction() throws Exception {
		DataSet<Tuple2<Double, Integer>> output = data.flatMap(new Averagenizer());
		output = output.sum(0).andSum(1);
		Tuple2<Double, Integer> finalTuple = output.collect().get(0);
		return finalTuple.f0 / finalTuple.f1;
	}
	
	/**
	 * 
	 * Multiply reaction time with user count and return tuple
	 *
	 */
	public static class Averagenizer implements FlatMapFunction<Tuple2<Double, Integer>, Tuple2<Double, Integer>>{

		private static final long serialVersionUID = 71246547222383551L;

		@Override
		public void flatMap(Tuple2<Double, Integer> value, Collector<Tuple2<Double, Integer>> out) {
			
			double reactionTime = value.f0;
			int userCount = value.f1;
			double reactionTimeSum = reactionTime * userCount;
			
			out.collect(new Tuple2<Double, Integer>(reactionTimeSum, userCount));
		}
		
	}
	
	/**
	 * 
	 * Get every reaction time with user count and return tuple
	 *
	 */
	public static class Medianizer implements FlatMapFunction<Tuple2<Double, Integer>, Tuple2<Double, Integer>>{

		private static final long serialVersionUID = 39387247222383551L;

		@Override
		public void flatMap(Tuple2<Double, Integer> value, Collector<Tuple2<Double, Integer>> out) {
			
			int userCount = value.f1;
			double reactionTime = value.f0;
			
			for(int i = 0; i < userCount; i++){
				out.collect(new Tuple2<Double, Integer>(reactionTime, 1));
			}
			
		}
		
	}

	public double getMedianReactionTime() throws Exception {
		DataSet<Tuple2<Double, Integer>> allReactionTests = data.flatMap(new Medianizer());

		List<Tuple2<Double, Integer>> reactionTimeList = allReactionTests.collect();
		
		double median = getMedianByCollection(reactionTimeList);
		
		return median;
	}

	/**
	 * Get the median by given list
	 * @param reactionTimeList the list containing reaction times
	 * @return the median of the reaction times
	 */
	private double getMedianByCollection(List<Tuple2<Double, Integer>> reactionTimeList) {
		double median = 0;
		if (reactionTimeList.size() % 2 == 0)
		    median = ((double) reactionTimeList.get(reactionTimeList.size()/2).f0 + (double) reactionTimeList.get(reactionTimeList.size() /2 - 1).f0)/2;
		else
		    median = (double) reactionTimeList.get(reactionTimeList.size()/2).f0;
		return median;
	}
}
