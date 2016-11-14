package stream;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;




public class MyKafkaStream {

	
	public void listenStream(){
		
		StreamExecutionEnvironment env = 
				  StreamExecutionEnvironment.getExecutionEnvironment();
				 
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		// only required for Kafka 0.8
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		DataStreamSink<String> stream = env
			.addSource(new FlinkKafkaConsumer08<>("topic", new SimpleStringSchema(), properties))
			.print();
			
	}
	
}
