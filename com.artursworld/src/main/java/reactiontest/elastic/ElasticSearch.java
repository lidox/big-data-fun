package reactiontest.elastic;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;


public class ElasticSearch {
	
	// CONFIGURATION
	public static final String ES_CLUSTER_NAME = "my-demo"; 
	public static final String ES_DOMAIN = "localhost";
	public static final int ES_PORT = 9300;
	
	public static final String ES_INDEX_NAME = "reactiontest";
	public static final String ES_TYPE_NAME = "reactiontest-log";
	
	
	private static Client client = null;
	
	public ElasticSearch() {
		try {
			Settings settings = Settings.settingsBuilder()
			        .put("cluster.name", ES_CLUSTER_NAME).build();
			client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ES_DOMAIN), ES_PORT));		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// TODO: delete?
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
	
	public void sinkToElasticSearch(DataStream<Tuple7<String, String, Integer, String, Date, String, List<Double>>> data){
		data.flatMap(new ElalsticSinker());
	}
	
	/**
	 * Sink a list of data into elastic search
	 * @param list the list to sink
	 * @param indexName the index name of elasticsearch to use
	 * @param typeName the type name of elasticsearch to use
	 */
	public void sinkHumanBenchmarkMyIndexAndTypeName(List<Tuple2<Double, Integer>> list, String indexName, String typeName){
		for(int i = 0; i < list.size(); i++){
			Map<String, Object> esJson = new HashMap<>();           
	        esJson.put("usercount", list.get(i).f1);
	        esJson.put("reactiontime", list.get(i).f0);
		    client.prepareIndex(indexName, typeName)  .setSource(esJson) .get();
		}
	}
	
	/**
	 * Attention! This inner static class must be used. Otherwise serialization problems appear
	 *
	 */
	public static class ElalsticSinker implements FlatMapFunction<Tuple7<String, String, Integer, String, Date, String, List<Double>> , Tuple7<String, String, Integer, String, Date, String, List<Double>>>{

		private static final long serialVersionUID = 8139075182383902551L;

		@Override
		public void flatMap(Tuple7<String, String, Integer, String, Date, String, List<Double>> tuple, Collector<Tuple7<String, String, Integer, String, Date, String, List<Double>>> output) {
			
			Map<String, Object> esJson = new HashMap<>();           
            esJson.put("medicalid", tuple.f0);
            esJson.put("operationissue", tuple.f1);
            esJson.put("age", tuple.f2.toString());
            esJson.put("gender", tuple.f3);
            SimpleDateFormat formater = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss.SSS");
            esJson.put("datetime", formater.format(tuple.f4).toString()); 
            esJson.put("type", tuple.f5);
            esJson.put("times", tuple.f6);

			// sink data
			client.prepareIndex(ES_INDEX_NAME, ES_TYPE_NAME)
			        .setSource(esJson)
			        .get();
			
			
			output.collect(tuple);
		}
	}
	
	/**
	 * Get Datastream by elasticsearch 
	 * @param env
	 * @return
	 * @throws Exception
	 */
	public DataStream<String> getStream(StreamExecutionEnvironment env) throws Exception {
		Collection<String> retList = getCollectionJSONStringsByElasticSearch(ES_INDEX_NAME, ES_TYPE_NAME);
		DataStream<String> textStream = env.fromCollection(retList);
		return textStream;	
	}

	/**
	 * Getting a collection of JSON strings
	 * @return
	 */
	public Collection<String> getCollectionJSONStringsByElasticSearch(String indexName, String typeName) {
		List<Map<String, Object>> maplist = getLisfOfMapsByIndexAndTypeName(indexName, typeName);
		Collection<String> retList = new ArrayList<String>();
		for(int i = 0; i < maplist.size(); i++){
			JSONObject obj = new JSONObject(maplist.get(i));
			StringBuilder sb = deleteBracketsAtArray(obj);
			retList.add(sb.toString());
		}
		return retList;
	}

	private StringBuilder deleteBracketsAtArray(JSONObject obj) {
		StringBuilder sb = new StringBuilder(obj.toString());
		int a = sb.indexOf("\"times\":") + 8;
		StringBuilder tmp = new StringBuilder(sb.toString().substring(a+1));
		int b = tmp.indexOf("\"");
		
		sb.deleteCharAt(a);
		sb.deleteCharAt(a+ b);
		return sb;
	}
	
	//private List<Map<String, Object>> getAllDocumentsFromElasticSearch(){
     //   return getLisfOfMapsByIndexAndTypeName(ES_INDEX_NAME, ES_TYPE_NAME);
	//}

	private List<Map<String, Object>> getLisfOfMapsByIndexAndTypeName(String indexName, String typeName) {
		int scrollSize = 1000;
        List<Map<String,Object>> esData = new ArrayList<Map<String,Object>>();
        SearchResponse response = null;
        int i = 0;
        while( response == null || response.getHits().hits().length != 0){
            response = client.prepareSearch(indexName)
                    .setTypes(typeName)
                       .setQuery(QueryBuilders.matchAllQuery())
                       .setSize(scrollSize)
                       .setFrom(i * scrollSize)
                    .execute()
                    .actionGet();
            for(SearchHit hit : response.getHits()){
                esData.add(hit.getSource());
            }
            i++;
        }
        return esData;
	}
	
}
