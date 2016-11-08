package reactiontest.elastic;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import reactiontest.online.ReactionTestStream;

//TODO: delete
public class MyElasticsearchSinkFunction implements ElasticsearchSinkFunction<String> {

	private static final long serialVersionUID = 9208617864747734287L;

	public IndexRequest createIndexRequest(String element) {
		//Tuple7<String, String, Integer, String, Date, String, List<Double>> tuple = ReactionTestStream.getTupleByJSON2(element);

        Map<String, String> esJson = new HashMap<>();
        String medicalid = "LENA????????????????????????????????????????";
        //String operationissue = tuple.f0;
        //String age = tuple.f0;
        //String gender = tuple.f0;
        //String datetime = tuple.f0;
        //String type = tuple.f0;
       //String times = tuple.f0;
        
        
        esJson.put("medicalid", medicalid);
        
        /*
        esJson.put("medicalid", tuple.f0);
        esJson.put("operationissue", tuple.f1);
        esJson.put("age", tuple.f2.toString());
        esJson.put("gender", tuple.f3);
        SimpleDateFormat formater = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss.SSS");
        esJson.put("datetime", formater.format(tuple.f4).toString()); 
        esJson.put("type", tuple.f5);
        String joined = Arrays.toString (tuple.f6.toArray());
        esJson.put("times", joined);
*/
        return Requests
                .indexRequest()
                .index(ElasticSearch.ES_INDEX_NAME)
                .type(ElasticSearch.ES_TYPE_NAME) 
                .source(esJson);
    }

    @Override
    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
        indexer.add(createIndexRequest(element));
    }

}
