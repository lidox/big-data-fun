package de.heinzen.christoph.streaming;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;

/**
 * Flink Job for the following exercise:
 * 
 * http://dataartisans.github.io/flink-training/exercises/rideCleansing.html
 *
 */
public class TaxiJob {

	public static void main(String[] args) throws Exception {
		
		// get an ExecutionEnvironment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// configure event-time processing
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// get the taxi ride data stream
		DataStream<TaxiRide> rides = env.addSource(
		  new TaxiRideSource("src/main/nycTaxiRides.gz", 5, 5));
		
		SingleOutputStreamOperator<TaxiRide> filteredRides = rides.filter(new NYCAreaFilter());
		
		filteredRides.print();

		// execute program
		env.execute("Flink Java Taxi Job");
	}
}
