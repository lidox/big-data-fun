package reactiontest.online;

public class MainOnline {

	public static void main(String[] args) throws Exception {
		ReactionTestStream stream = new ReactionTestStream();
		stream.initKafkaConsumer();
		
		// metric: count 
		int messageCount = stream.getCount();
		System.err.println("1. RT  message Count: " + messageCount); 

	}

}
