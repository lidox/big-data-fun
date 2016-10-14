package batch;

import kafka.Kafka;
import kafka.consumer.KafkaStream;
import stream.MyKafkaStream;
import stream.TaxiStreamProcessor;

public class Main {
	
	
    static String csvHomePath = "/home/lidox/Dokumente/bigdata-workspace/big-data-fun/flink-processing-java/data/";
	
	public static void main(String[] args) throws Exception {
		
		
		// String csvPath = csvHomePath + "flinkMails.gz";
		// MailBatchProcessor batch = new MailBatchProcessor(csvPath);
		// batch.getUserAndMailCount();
		
		//String taxiCsvPath = csvHomePath + "nycTaxiRides.gz";
		//TaxiStreamProcessor stream = new TaxiStreamProcessor(taxiCsvPath);
		//stream.test();
		
		MyKafkaStream stream = new MyKafkaStream();
		stream.listenStream();
	}

}
