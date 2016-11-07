package reactiontest.elastic;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import reactiontest.online.ReactionTestStream;


public class ElasticSearch {
	
	// CONFIGURATION
	public static final String ES_CLUSTER_NAME = "my-demo"; 
	public static final String ES_DOMAIN = "localhost";
	public static final int ES_PORT = 9300;
	
	public static final String ES_INDEX_NAME = "reactiontest";
	public static final String ES_TYPE_NAME = "reactiontest-log";
	
	
	private Client client = null;
	
	public ElasticSearch() throws UnknownHostException {
		Settings settings = Settings.settingsBuilder()
		        .put("cluster.name", ES_CLUSTER_NAME).build();
		client = TransportClient.builder().settings(settings).build()
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ES_DOMAIN), ES_PORT));
	}

	public void search(String column, String textToSearch){
		SearchResponse response = client.prepareSearch(ES_INDEX_NAME)
		        .setTypes(ES_TYPE_NAME)
		        .setQuery(QueryBuilders.matchQuery(column, textToSearch))
		        .execute()
		        .actionGet();
		
		System.out.println(response);
		
        SearchHit[] results = response.getHits().getHits();
        for(SearchHit hit : results){

            String sourceAsString = hit.getSourceAsString();
            if (sourceAsString != null) {
                //Gson gson = new GsonBuilder().setDateFormat(dateFormat)
                        //.create(); 
            	//System.out.println(sourceAsString);
                //System.out.println( gson.fromJson(sourceAsString, Firewall.class));
            }
        }
	}
	
	public void closeConenction(){
		client.close();
	}
	
	
    /*
			"medicalid":"Markus",
			"operationissue":"foobar",
			"age":54,
			"gender":"Male",
			"datetime":"2016-11-03 20:59:28.807",
			"type":"PreOperation",
			"times":[412,399,324]
		*/    
    public void writeToElastic(DataStream<String> input) {

        Map<String, String> config = new HashMap<>();
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", ES_CLUSTER_NAME);

        try {
            // Add elastic search hosts on startup
            List<InetSocketAddress> transports = new ArrayList<>();
            transports.add(new InetSocketAddress(ES_DOMAIN, ES_PORT));
            ElasticsearchSinkFunction<String> indexLog = new MyElasticsearchSinkFunction();
            /*
            ElasticsearchSinkFunction<String> indexLog = new ElasticsearchSinkFunction<String>() {

				private static final long serialVersionUID = 9208617864747734287L;

				public IndexRequest createIndexRequest(String element) {
            		//Tuple7<String, String, Integer, String, Date, String, List<Double>> tuple = ReactionTestStream.getTupleByJSON2(element);

                    Map<String, String> esJson = new HashMap<>();
                    esJson.put("medicalid", element);
                    
                    esJson.put("medicalid", tuple.f0);
                    esJson.put("operationissue", tuple.f1);
                    esJson.put("age", tuple.f2.toString());
                    esJson.put("gender", tuple.f3);
                    SimpleDateFormat formater = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss.SSS");
                    esJson.put("datetime", formater.format(tuple.f4).toString()); 
                    esJson.put("type", tuple.f5);
                    String joined = Arrays.toString (tuple.f6.toArray());
                    esJson.put("times", joined);

                    return Requests
                            .indexRequest()
                            .index(ES_INDEX_NAME)
                            .type(ES_TYPE_NAME) 
                            .source(esJson);
                }

                @Override
                public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                    indexer.add(createIndexRequest(element));
                }
            };
            */

            ElasticsearchSink<String> esSink = new ElasticsearchSink<String>(config, transports, indexLog);
            input.addSink(esSink);
        } catch (Exception e) {
        	e.printStackTrace();
        }
        
    }
	
}
