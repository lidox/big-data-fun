package reactiontest.online;

public class MainOnline {

	public static void main(String[] args) throws Exception {
		ReactionTestStream stream = new ReactionTestStream();
		stream.initKafkaConsumer();

	}

}
