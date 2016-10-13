package mailexample;

public class Main {
	
	
	public static void main(String[] args) throws Exception {
		String csvPath = "/home/osboxes/Desktop/apache-flink/big-data-fun/flink-batch-processing-java/data/flinkMails.gz";
		MailBatchProcessor batch = new MailBatchProcessor(csvPath);
		batch.getUserAndMailCount();
	}

}
