package mailexample;

public class Main {
	
	
	public static void main(String[] args) throws Exception {
		String csvPath = "/home/lidox/Dokumente/bigdata-workspace/big-data-fun/flink-processing-java/data/flinkMails.gz";
		MailBatchProcessor batch = new MailBatchProcessor(csvPath);
		batch.getUserAndMailCount();
	}

}
