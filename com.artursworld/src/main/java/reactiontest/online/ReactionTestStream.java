package reactiontest.online;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

public class ReactionTestStream {
	
    // configuration
    private static String KAFKA_SERVER_DOMAIN = "localhost"; 
    private static String KAFKA_SERVER_PORT = "9092"; 
    private static String KAFKA_TOPIC_GROUP_ID = "reactiontest"; 
    
    private static String ZOOKEEPER_SERVER_PORT = "2181"; 
    private static String ZOOKEEPER_SERVER_DOMAIN = "localhost";
	
	// stream processing
    private StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private DataStream<String> textStream = null;
    
	/**
	 * Initializes a Kafka consumer
	 * @throws Exception 
	 */
	public void initKafkaConsumer() throws Exception {
		textStream = readFromKafka(env);

		textStream.flatMap(new FlatMapFunction<String, Tuple7<String, String, Integer, String, Date, String, List<Double>>>() {
	    	
		    private static final long serialVersionUID = 368385747690202L;

			@Override
			public void flatMap(String jsonString, Collector<Tuple7<String, String, Integer, String, Date, String, List<Double>>> out) throws Exception {
				Tuple7<String, String, Integer, String, Date, String, List<Double>> jsonTuple = getTupleByJSON(jsonString); 
				out.collect(jsonTuple);
			}
			
			public Tuple7<String, String, Integer, String, Date, String, List<Double>> getTupleByJSON(String jsonString){    
				
				try {
					/**
					 * {
						"medicalid":"Markus",
						"operationissue":"blablub",
						"age":54,
						"gender":"Male",
						"datetime":"2016-11-03 20:59:28.807",
						"type":"PreOperation",
						"times":[412,399,324]
					}
					 */
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
				}
				return new Tuple7<String, String, Integer, String, Date, String, List<Double>>();
			}


		    }).print();
		
		env.execute();
	}
	
    public DataStream<String> readFromKafka(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_SERVER_DOMAIN+":"+KAFKA_SERVER_PORT);
        properties.setProperty("group.id", KAFKA_TOPIC_GROUP_ID);
        properties.setProperty("zookeeper.connect", ZOOKEEPER_SERVER_DOMAIN+":"+ZOOKEEPER_SERVER_PORT);
        
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer08<>(KAFKA_TOPIC_GROUP_ID, new SimpleStringSchema(), properties));
        return stream;
    }

}
