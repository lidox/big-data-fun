package elasticsearch;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;




import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;

public class ElasticSink {

	
	private StreamExecutionEnvironment env = null;
	private String csvPath = null;
	private final int maxEventDelay = 60;       // events are out of order by max 60 seconds
	private final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second
	private DataStream<TaxiRide> input;  
	
	// config
	public static final String INDEX_NAME = "nyc-idx"; 
	public static final String CLUSTER_NAME = "my-demo";
	
	public ElasticSink(String path) {
		this.env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		this.csvPath = path;
		
		// configure event-time processing
		this.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		// get the taxi ride data stream
		input = env.addSource(new TaxiRideSource(this.csvPath, maxEventDelay, servingSpeedFactor));
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
	

}
