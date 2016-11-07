package reactiontest.elastic;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;


public class ElasticSearch {
	
	private Client client = null;
	
	public ElasticSearch() throws UnknownHostException {
		Settings settings = Settings.settingsBuilder()
		        .put("cluster.name", "my-demo").build();
		client = TransportClient.builder().settings(settings).build()
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
	}

	public void search(){
		//curl 'localhost:9200/viper-test/viper-log/_search?regina'
		SearchResponse response = client.prepareSearch("viper-test")
		        .setTypes("viper-log")
		        .setQuery(QueryBuilders.matchQuery("second", "regina"))
		        //.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        //.setQuery(QueryBuilders.termQuery("regina", "test"))                 // Query
		        //.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
		        //.setFrom(0).setSize(60).setExplain(true)
		        .execute()
		        .actionGet();
		System.out.println(response);
        SearchHit[] results = response.getHits().getHits();
        for(SearchHit hit : results){

            String sourceAsString = hit.getSourceAsString();
            if (sourceAsString != null) {
                //Gson gson = new GsonBuilder().setDateFormat(dateFormat)
                        //.create(); 
            	System.out.println(sourceAsString);
                //System.out.println( gson.fromJson(sourceAsString, Firewall.class));
            }
        }
		//GetResponse response2 = client.prepareGet("viper-test", "viper-log", "regina").get();
		//System.out.println(response2);
	}
	
	public void closeConenction(){
		client.close();
	}
	
	public static void main(String[] args) throws UnknownHostException {
		ElasticSearch e = new ElasticSearch();
		e.search();
		
	}
	
}
