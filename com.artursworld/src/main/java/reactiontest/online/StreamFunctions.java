package reactiontest.online;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import reactiontest.elastic.ElasticSearch;

/**
{
	"medicalid":"Markus",
	"operationissue":"foobar",
	"age":54,
	"gender":"Male",
	"datetime":"2016-11-03 20:59:28.807",
	"type":"PreOperation",
	"times":[412,399,324]
}
*/
public class StreamFunctions {
	
    // configuration
	public static String KAFKA_SERVER_DOMAIN = "localhost"; 
    public static String KAFKA_SERVER_PORT = "9092"; 
    public static String KAFKA_TOPIC_GROUP_ID = "reactiontest"; 
    public static String ZOOKEEPER_SERVER_PORT = "2181"; 
    public static String ZOOKEEPER_SERVER_DOMAIN = "localhost";
	
	// stream processing
    public StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //public DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data = null; 
    
    // metrics
    private OnlineMetrics metrics = new OnlineMetrics();
    
    
 	public void print(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data){
		data.print();
 	}
 	
 	public void execute() throws Exception{
 		env.execute();
 	}
 	
	/**
	 * Initializes a Kafka consumer
	 * @throws Exception 
	 */
	public DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> getKafkaStream() throws Exception { 
		DataStream<String> textStream = readFromKafka(env);
		DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> result = null;
		result = textStream.flatMap(new String2TupleFlatMapFunction());
		
		new ElasticSearch().sinkToElasticSearch(result);
		
		return textStream.flatMap(new String2TupleFlatMapFunction());
	}
	

	public static class String2TupleFlatMapFunction implements FlatMapFunction<String , Tuple7<String, String, Integer, String, Date, String, List<Double>>>{

		private static final long serialVersionUID = 3465342383902551L;

		@Override
		public void flatMap(String jsonString, Collector<Tuple7<String, String, Integer, String, Date, String, List<Double>>> out) {
			Tuple7<String, String, Integer, String, Date, String, List<Double>> jsonTuple = getTupleByJSON2(jsonString); 
			out.collect(jsonTuple);
		}
		
	}
	
    public DataStream<String> readFromKafka(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_SERVER_DOMAIN+":"+KAFKA_SERVER_PORT);
        properties.setProperty("group.id", KAFKA_TOPIC_GROUP_ID);
        properties.setProperty("zookeeper.connect", ZOOKEEPER_SERVER_DOMAIN+":"+ZOOKEEPER_SERVER_PORT);
        
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer08<>(KAFKA_TOPIC_GROUP_ID, new SimpleStringSchema(), properties));
        return stream;
    }

    /**
     * Calculate the count of reaction tests
     * @param time the tumbling time window to be used
     */
	public void printCount(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data, Time time) {
		metrics.setTimeWindow(time);  
		metrics.getCount(data).print();
	}
	
	/**
	 * Calculate the average reaction time
	 * @param time the tumbling time window to be used
	 */
	public void printAverage(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data, Time time) {
		metrics.setTimeWindow(time);  
		metrics.getAverageReactionTime(data).print();
	}


	/**
	 * Prints the median of by the specified time window
	 * @param seconds
	 */
	public void printMedianByTimeWindow(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data, Time time) {
		metrics.setTimeWindow(time);  
		metrics.getMedianReactionTime(data).print();
	}

	public void printMinMaxByTimeWindow(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data, Time time, int value) {
		metrics.setTimeWindow(time);  
		metrics.getMinMaxReactionTimeByTimeWindow(data,value).print();
	}

	/**
	 * Reads a DataStream<String> from Kafka and sinks it
	 * @throws UnknownHostException
	 */
	public static void sinkToElasticSearch(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data) throws Exception {
		ElasticSearch elastic = new ElasticSearch();
		elastic.sinkToElasticSearch(data);
		System.out.println("SUCCESS sink"); 
	}
	
	public static Tuple7<String, String, Integer, String, Date, String, List<Double>> getTupleByJSON2(
			String jsonString)  {
		try {
		JSONObject request = new JSONObject(jsonString);
		String medicalid = request.getString("medicalid"); 
		String operationissue = request.getString("operationissue"); 
		int age = Integer.parseInt(request.getString("age")); 
		String gender = request.getString("gender"); 
		Date datetime = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss.SSS").parse(request.getString("datetime")); 
		String type = request.getString("type"); 
		JSONArray timesArray = request.getJSONArray("times"); 
		List<Double> times = new ArrayList<Double>();
		for(int i = 0; i < timesArray.length(); i++){
			times.add(Double.parseDouble(timesArray.getString(i)));
		}
			
		return new Tuple7<String, String, Integer, String, Date, String, List<Double>>(
				medicalid,operationissue,age,gender,datetime,type,times);			
		} catch (Exception e) {
			e.printStackTrace();
			return new Tuple7<String, String, Integer, String, Date, String, List<Double>>();		
		}
	}
	

	/**
	 * Reads data from elasticsearch into a DataStream
	 * @throws Exception
	 */
	public DataStream<String> getElasticSearchStream() throws Exception {
		ElasticSearch elastic = new ElasticSearch(); 
		DataStream<String> textStream = elastic.getStream(env);
		//DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> retData = textStream.flatMap(new String2TupleFlatMapFunction());
		//data.print();
		env.execute();
		return textStream;
	}

	public void printPredictionForNextReactionTimeByAVGs(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data, double average, Time time) {
		metrics.setTimeWindow(time);  
		SingleOutputStreamOperator<Tuple2<String, Double>> result = metrics.getPredictedReactionTimeByAVGs(data, average);
		result.print();
	}

	public void printPredictionForNextReactionTimeByMedians(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data,double median,Time time) {
		metrics.setTimeWindow(time);  
		metrics.getPredictedReactionTimeByMedians(data, median).print();
	}

	public void printCount(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data,Time time, Time slideTime) {
		metrics.setTimeWindow(time);  
		metrics.getCount(data, slideTime).print();	
	}
	
	public void printPredictionForNextReactionTimeBySlidingAVGs(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data,double average, Time time, Time slide) {
		metrics.setTimeWindow(time);  
		metrics.getPredictedReactionTimeBySlidingAVGs(data, average, slide).print();
	}

}
