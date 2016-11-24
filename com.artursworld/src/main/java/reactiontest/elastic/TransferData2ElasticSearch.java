package reactiontest.elastic;

import java.net.UnknownHostException;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

import reactiontest.offline.BatchFunctions;

public class TransferData2ElasticSearch {
	
	public static void main(String[] args) {
		try {

			BatchFunctions batch = new BatchFunctions();
			writeHumanBenchmarkToES(batch);

			//getRecordsFromES();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void writeHumanBenchmarkToES(BatchFunctions batch)
			throws Exception, UnknownHostException {
		DataSet<Tuple2<Double, Integer>> dataSet = batch.loadDataSetOfAllTime();
		List<Tuple2<Double, Integer>> list = dataSet.collect();
		ElasticSearch es = new ElasticSearch();
		String indexName = "humanbenchmark";
		String typeName = "reactiontests";
		es.sinkHumanBenchmarkMyIndexAndTypeName(list, indexName, typeName);
	}

	/*
	private static void getRecordsFromES() throws UnknownHostException {
		ElasticSearch s = new ElasticSearch();
		Collection<String> collection= s.getCollectionByFromElasticSearch();
		String st = collection.toString();
		System.out.println(st);
	}
	*/
	
}
