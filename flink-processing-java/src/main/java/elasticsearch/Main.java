package elasticsearch;


public class Main {

	static String csvHomePath = "/home/lidox/Dokumente/bigdata-workspace/big-data-fun/flink-processing-java/data/";
	
	public static void main(String[] args) {
		try {
			
			
			 String csvPath = csvHomePath + "nycTaxiRides.gz";
			 
			 // loading DataStream
			 ElasticSink elastic = new ElasticSink(csvPath);
			 
			 elastic.print();
			 
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
