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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.java file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
public class Job {

	public static void main(String[] args) throws Exception {
		
		// get an ExecutionEnvironment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// configure event-time processing
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// get the taxi ride data stream
		DataStream<TaxiRide> rides = env.addSource(
		  new TaxiRideSource("/home/christoph/Dokumente/Big Data/material/nycTaxiRides.gz", 5, 5));//, maxDelay, servingSpeed));
		
		SingleOutputStreamOperator<TaxiRide> filteredRides = rides.filter(new FilterFunction<TaxiRide>() {
			@Override
			public boolean filter(TaxiRide arg0) throws Exception {
				return GeoUtils.isInNYC(arg0.endLon, arg0.endLat) && GeoUtils.isInNYC(arg0.startLon, arg0.startLat);
			}
		});
		
		filteredRides.print();
		
		

		// execute program
		env.execute("Flink Java API Skeleton");
	}
}
