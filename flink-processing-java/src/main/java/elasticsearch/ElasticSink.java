package elasticsearch;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import model.TaxiRide;
import model.TaxiRideSource;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.Interval;

import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaIntervalSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaLocalDateTimeSerializer;

public class ElasticSink {

	
	private StreamExecutionEnvironment env = null;
	private String csvPath = null;
	private final int maxEventDelay = 60;       // events are out of order by max 60 seconds
	private final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second
	private DataStream<TaxiRide> input;  
	private DataStream<String> viperStream;  
	
	// config
	public static final String INDEX_NAME = "nyc-idx"; 
	public static final String CLUSTER_NAME = "my-demo";
	
	public ElasticSink(String path) {
		this.env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.registerTypeWithKryoSerializer(DateTime.class, JodaDateTimeSerializer.class);
		env.registerTypeWithKryoSerializer(Interval.class, JodaIntervalSerializer.class);
		
		this.csvPath = path;
		
		// configure event-time processing
		this.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		// get the taxi ride data stream
		input = env.addSource(new TaxiRideSource(this.csvPath, maxEventDelay, servingSpeedFactor));
		//env.registerTypeWithKryoSerializer([LocalDateTime], classOf[JodaLocalDateTimeSerializer]); 
	}
	
	/**
	 * Prints the DataStream to console
	 */
	public void print(){
		if(input != null)
			try {
				input.print();
				env.execute("New Yorker Taxi Rides");
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
	
	public void sink(){
	    try {
			Map<String, String> config = Maps.newHashMap();
			// This instructs the sink to emit after every element, otherwise they would be buffered
			config.put("bulk.flush.max.actions", "1");
			config.put("cluster.name", CLUSTER_NAME);
	
		    List<InetSocketAddress> transports = new ArrayList<>();
		    transports.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300));
	
		    input.addSink(new ElasticsearchSink<TaxiRide>(config, transports, new TestElasticsearchSinkFunction()));
	
		
			env.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Sink String stream from kafka to elasticsearch
	 */
	public void sink2(){
	    try {
	    	writeElastic(input);
			//env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void writeElastic(DataStream<TaxiRide> input) {
		// get Stream
		viperStream = getKafkaStream(env);
		
	    Map<String, String> config = new HashMap<>();

	    //TODO: hier weitermachen! clustername: try to sink simple streams
	    // This instructs the sink to emit after every element, otherwise they would be buffered
		config.put("bulk.flush.max.actions", "1");
		config.put("cluster.name", CLUSTER_NAME);

	    try {
	        // Add elasticsearch hosts on startup
	        List<InetSocketAddress> transports = new ArrayList<>();
	        transports.add(new InetSocketAddress("127.0.0.1", 9300)); // port is 9300 not 9200 for ES TransportClient

	        ElasticsearchSinkFunction<TaxiRide> indexLog = new ElasticsearchSinkFunction<TaxiRide>() {
	            public IndexRequest createIndexRequest(TaxiRide element) {
	                //String[] logContent = element.toString().trim().split("\t");
	                Map<String, String> esJson = new HashMap<>();
	                esJson.put("endLat", element.endLat + "");
	                esJson.put("isStart", element.isStart+"");

	                return Requests
	                		//curl -XPUT 'localhost:9200/viper-test/_mapping/viper-log' -d '{
	                		// curl -XPUT "http://localhost:9200/nyc-idx/_mapping/popular-locations" -d'
	                        .indexRequest()
	                        .index("nyc-idx")
	                        .type("popular-locations")
	                        .source(esJson);
	            }

	            @Override
	            public void process(TaxiRide element, RuntimeContext ctx, RequestIndexer indexer) {
	                indexer.add(createIndexRequest(element));
	            }
	        };

	        ElasticsearchSink esSink = new ElasticsearchSink(config, transports, indexLog);
	        input.addSink(esSink);
	    } catch (Exception e) {
	        System.out.println(e);
	    }
	}
	
	
	// http://stackoverflow.com/questions/37514624/apache-flink-integration-with-elasticsearch
	// https://ci.apache.org/projects/flink/flink-docs-master/dev/connectors/elasticsearch.html
	private static class TestElasticsearchSinkFunction implements ElasticsearchSinkFunction<TaxiRide> {
	    private static final long serialVersionUID = 1L;

	    public IndexRequest createIndexRequest(String element) {
	        Map<String, Object> json = new HashMap<>();
	        json.put("data", element);

	        return Requests.indexRequest()
	                .index(INDEX_NAME)
	                .id("hash"+element)
	                .source(json);
	    }
	    


	    @Override
	    public void process(TaxiRide element, RuntimeContext ctx, RequestIndexer indexer) {
	        indexer.add(createIndexRequest(element.toString()));
	    }

	}
	
	/**
	 * Get the simple String Stream from Kafka
	 * @param env the running environment
	 * @return the new DataStream by Kafka
	 */
	public DataStream<String> getKafkaStream(StreamExecutionEnvironment env) {
	    env.enableCheckpointing(5000); 
	    Properties properties = new Properties();
	    properties.setProperty("bootstrap.servers", "localhost:9092"); 
	    properties.setProperty("group.id", "test");

	    DataStream<String> stream= env.addSource(
	            new FlinkKafkaConsumer09<>("test", new SimpleStringSchema(), properties));
	    return stream;
	}
	

}
