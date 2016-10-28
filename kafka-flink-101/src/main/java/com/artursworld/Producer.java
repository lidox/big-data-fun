package com.artursworld;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
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
public class Producer {

	
	public static void main(String[] args) throws Exception {
	    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	    Properties properties = new Properties();
	    properties.setProperty("bootstrap.servers", "localhost:9092"); 
	    //properties.setProperty("bootstrap.servers", "134.99.218.18:443"); 
	    DataStream<String> stream = env.addSource(new SimpleStringGenerator());
	    stream.addSink(new FlinkKafkaProducer09<>("flink-demo", new SimpleStringSchema(), properties));

	    env.execute();
	  }
	
	  /**
	   * Simple Class to generate data
	   */
	  public static class SimpleStringGenerator implements SourceFunction<String> {
	    private static final long serialVersionUID = 119007289730474249L;
	    boolean running = true;
	    long i = 0;
	    @Override
	    public void run(SourceContext<String> ctx) throws Exception {
	      while(running) {
	    	  
	    	  
	        ctx.collect("FLINK-KAFKA try: "+ (i++));
	        Thread.sleep(1000);
	      }
	      
	    }
	    @Override
	    public void cancel() {
	      running = false;
	    }
	  }
	
}
