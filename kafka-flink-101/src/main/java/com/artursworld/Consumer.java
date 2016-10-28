package com.artursworld;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Example based on following blog post: https://www.javacodegeeks.com/2016/10/getting-started-apache-flink-kafka.html
 * 
 * Kafka version: kafka_2.11-0.9.0.0
 * Flink version: 1.1.2
 * 
 * @author lidox
 *
 */
public class Consumer {

	public static void main(String[] args) throws Exception {
	    // create execution environment
	    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	    Properties properties = new Properties();
	    properties.setProperty("bootstrap.servers", "localhost:9092");
	    //properties.setProperty("bootstrap.servers", "134.99.218.18:443"); 
	   
	    properties.setProperty("group.id", "flink_consumer");

	    DataStream<String> stream = env.addSource(new FlinkKafkaConsumer09<>(
	        "test", new SimpleStringSchema(), properties) );

	    stream.map(new MapFunction<String, String>() {
	    	
	      private static final long serialVersionUID = 787878747690202L;

	      @Override
	      public String map(String value) throws Exception {
	        return "Stream Value: " + value;
	      }
	    }).print();

	    env.execute();
	  }
	
}
