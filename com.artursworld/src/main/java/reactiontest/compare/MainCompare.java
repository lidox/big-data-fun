package reactiontest.compare;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import reactiontest.elastic.ElasticSearch;
import reactiontest.offline.BatchFunctions;
import reactiontest.online.StreamFunctions;
import reactiontime.utils.Statistics;

/**
 * Compare the online statistics with the offline computed statistics 
 * (integrate in the online analysis the offline values) 
 * and make predictions
 */
public class MainCompare {

	public static void main(String[] args) throws Exception {
		
		// Condition for following metrics:
		// 1. running Apache Kafka
		// 2. insert data to Kafka by copy/paste following JSON string:
		// {"medicalid":"Fabio", "operationissue":"italia", "age":33, "gender":"Male", "datetime":"2016-11-03 20:59:28.807", "type":"PreOperation", "times":[141,1750,2000] }
		
		
		// Prediction 1:
		printPredictionByAverage();
		
		// Prediction 2:
		printPredictionByAVGofMedians();
		
		// Prediction 3:
		printPredictionBySlidingAverage();
		
	}

	/**
	 * The prediction algorithm: 
	 * 1. calculate median of reaction times from offline human benchmark data using JSON files
	 * 2. calculate median of reaction times from own reaction time data using ElastiSearch
	 * 3. calculate the average of both medians
	 * 4. calculate the median of the data coming from the stream using thumbling window
	 * 5. Prediction for the next RT: Average of 4. and 3.
	 * @throws Exception
	 */
	private static void printPredictionByAVGofMedians() throws Exception {
		// Prediction 2: Median off all reaction data since October 2016 
		// and last reaction times specified by tumbling window
		BatchFunctions batch = new BatchFunctions();
		
		// first get median of reaction times by offline data
		DataSet<Tuple2<Double, Integer>> dataSet1 = batch.loadDataSetOfOctober2016();
		double median = batch.getMedianReactionTime(dataSet1);
		
		// now get own offline data from elastic search 
		Statistics stats = getOfflineDataByElasticSearch();
		double medianES = stats.getMedian();
		median = (median + medianES) / 2; // average of the medians
		
		// second use calculated median to make a prediction
		StreamFunctions stream = new StreamFunctions();
		DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> dataStream1 = stream.getKafkaStream();
		
		// prediction for next reaction time = Median of the offlineAverage and onlineAverage using tumbling window
		stream.printPredictionForNextReactionTimeByMedians(dataStream1, median, Time.seconds(10));
		stream.execute();
	}
	
	/**
	 * The prediction algorithm: 
	 * 1. calculate mean of reaction times from offline human benchmark data using JSON files
	 * 2. calculate mean of reaction times from own reaction time data using ElastiSearch
	 * 3. calculate the mean of both means
	 * 4. calculate the mean of the data coming from the stream using thumbling window
	 * 5. Prediction for the next RT: Average of 4. and 3.
	 * @throws Exception
	 */
	private static void printPredictionByAverage() throws Exception {
		// Prediction 1: Average off October 2016 reaction data + tumbling window
		
		// first get average reaction time by offline data by human benchmark
		BatchFunctions batch = new BatchFunctions(); 
		DataSet<Tuple2<Double, Integer>> dataSet1 = batch.loadDataSetOfOctober2016();
		double average = batch.getAverageReaction(dataSet1);
		
		// now get own offline data from elastic search 
		Statistics stats = getOfflineDataByElasticSearch();
		double averageES = stats.getMean();
		average = (average + averageES) / 2;
		/*
		String indexName = "humanbenchmark";
		String typeName = "reactiontests";
		Collection<String> stringCollectionHuman = es.getCollectionJSONStringsByElasticSearch(indexName, typeName);
		List<Tuple2<Double, Integer>> humanList = getHumanReactionTimesByES(stringCollectionHuman);
		double humanAVERAGE = getAverageReactionTimeByList(humanList);
		*/
		
		// second use calculated average to make a prediction
		StreamFunctions stream = new StreamFunctions();
		DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> dataStream1 = stream.getKafkaStream();
		
		// prediction for next reaction time = Mean of the offlineAverage and onlineAverage using tumbling window
		stream.printPredictionForNextReactionTimeByAVGs(dataStream1, average, Time.seconds(10));
		stream.execute();
	}

	private static Statistics getOfflineDataByElasticSearch() {
		ElasticSearch es = new ElasticSearch();
		Collection<String> collection = es.getCollectionJSONStringsByElasticSearch(ElasticSearch.ES_INDEX_NAME, ElasticSearch.ES_TYPE_NAME);
		List<Double> allReactionTimesByES = getReactionTimesByES(collection);
		Statistics stats = new Statistics(allReactionTimesByES);
		return stats;
	}

	/*
	private static double getAverageReactionTimeByList(List<Tuple2<Double, Integer>> humanList) {
		int globalUserCount = 0;
		double globalReactionTime = 0;
		for(Tuple2<Double, Integer> item : humanList){
			globalUserCount += item.f1;
			globalReactionTime += item.f0;
		}
		return globalReactionTime / globalUserCount;
	}
	*/

	private static List<Double> getReactionTimesByES(Collection<String> collection) {
		List<Double> allReationTimesFromES = new ArrayList<Double>();
		for (java.util.Iterator<String> i = collection.iterator(); i.hasNext(); ){
			String jsonString = i.next();
			Tuple7<String, String, Integer, String, Date, String, List<Double>> tuple = StreamFunctions.getTupleByJSON2(jsonString);
			allReationTimesFromES.addAll(tuple.f6);
		}
		return allReationTimesFromES;
	}
	
	/*
	private static List<Tuple2<Double, Integer>> getHumanReactionTimesByES(Collection<String> collection) {
		List<Tuple2<Double, Integer>> allReationTimesFromES = new ArrayList<>();
		for (java.util.Iterator<String> i = collection.iterator(); i.hasNext(); ){
			String jsonString = i.next();
			try {
				JSONObject o = new JSONObject(jsonString);
				double rt = o.getDouble("reactontime");
				int userCount = o.getInt("usercount");
				allReationTimesFromES.add(new Tuple2<Double, Integer>(rt, userCount));
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		return allReationTimesFromES;
	}
	*/
	
	/**
	 * The prediction algorithm: 
	 * 1. calculate median of reaction times from offline human benchmark data using JSON files
	 * 2. calculate median of reaction times from own reaction time data using ElastiSearch
	 * 3. calculate the average of both medians
	 * 4. calculate the median of the data coming from the stream using sliding window
	 * 5. Prediction for the next RT: mean of 4. and 3.
	 * @throws Exception
	 */
	private static void printPredictionBySlidingAverage() throws Exception {
		// Prediction 3: Average off October 2016 reaction data + tumbling window
		
		// first get average reaction time by offline data
		BatchFunctions human = new BatchFunctions();
		DataSet<Tuple2<Double, Integer>> dataSet1 = human.loadDataSetOfOctober2016();
		double average = human.getAverageReaction(dataSet1);
		
		// now get own offline data from elastic search 
		Statistics stats = getOfflineDataByElasticSearch();
		double averageES = stats.getMean();
		average = (average + averageES) / 2;
		
		// second use calculated average to make a prediction
		StreamFunctions stream = new StreamFunctions();
		DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> dataStream1 = stream.getKafkaStream();
		// prediction for next reaction time = Mean of the offlineAverage and onlineAverage using sliding window
		stream.printPredictionForNextReactionTimeBySlidingAVGs(dataStream1, average, Time.seconds(10), Time.seconds(3));
		stream.execute();
	}

}
