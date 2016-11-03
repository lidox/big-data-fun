package reactiontest.offline;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
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
			DataSet<String> inputData = env.readTextFile(getFilePath(filePathJSON));
			data = inputData.flatMap(new Tokenizer());
		} catch(Exception e) {
			  e.printStackTrace();
		}
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
	
	/*
	DataSet<Tuple2<Double, Integer>> data = inputData.map(new MapFunction<String, Tuple2<Double, Integer>>() {

		private static final long serialVersionUID = -3098918429893723175L;

		@Override
		public Tuple2<Double, Integer> map(String value) throws Exception {
			Double reactionTime = -1.;
			Integer userCount = -1;
			try {
				//reactionTime = Double.parseDouble(value.f0);
				//userCount = Integer.parseInt(value.f1);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return new Tuple2<Double, Integer>(reactionTime, userCount);
		}
	});
	*/
}
