package de.heinzen.christoph.wordCount;

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

import java.util.Iterator;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

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
public class MailCount {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<String, String>> mailsTmp =
				  env.readCsvFile("/home/christoph/Dokumente/Big Data/material/flinkMails.gz")
				    .lineDelimiter("##//##")
				    .fieldDelimiter("#|#").includeFields("011000")
				    .types(String.class, String.class);
		
		DataSet<Tuple3<String, String, Integer>> mails = mailsTmp.map(new MapFunction<Tuple2<String,String>, Tuple3<String, String, Integer>>() {
			@Override
			public Tuple3<String,String, Integer> map(Tuple2<String, String> arg) throws Exception {
				int start = arg.f1.lastIndexOf('<');
				int end = arg.f1.lastIndexOf('>');
				return new Tuple3<String, String, Integer>(arg.f0.substring(0, 7), arg.f1.substring(start+1, end), 1);
			}
		});		
		
		DataSet<Tuple2<String, Integer>> mailCount = mails.reduce(new ReduceFunction<Tuple3<String,String,Integer>>() {
			@Override
			public Tuple3<String, String, Integer> reduce(
					Tuple3<String, String, Integer> arg0,
					Tuple3<String, String, Integer> arg1) throws Exception {
				return new Tuple3<String, String, Integer>("","",arg0.f2+arg1.f2);
			}
		}).map(new MapFunction<Tuple3<String,String,Integer>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(Tuple3<String, String, Integer> arg0) throws Exception {
				return new Tuple2<String, Integer>("total mail count", arg0.f2);
			}
		});
		
		DataSet<Tuple2<String, Integer>> senderCount = mails.groupBy(1).reduceGroup(new GroupReduceFunction<Tuple3<String,String,Integer>, Tuple2<String, Integer>>() {
			@Override
			public void reduce(Iterable<Tuple3<String, String, Integer>> arg0, Collector<Tuple2<String, Integer>> arg1) throws Exception {
				Iterator<Tuple3<String, String, Integer>> iterator = arg0.iterator();
				if(iterator.hasNext()) {
					Tuple3<String, String, Integer> tuple = iterator.next();
					arg1.collect(new Tuple2<String, Integer>(tuple.f1, 1));
				}
			}
		}).reduce(new ReduceFunction<Tuple2<String,Integer>>() {
			@Override
			public Tuple2<String, Integer> reduce(Tuple2<String, Integer> arg0,	Tuple2<String, Integer> arg1) throws Exception {
				return new Tuple2<String, Integer>("", arg0.f1 + arg1.f1);
			}
		}).map(new MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>>() {
			@Override
			public Tuple2<String, Integer> map(Tuple2<String, Integer> arg0) throws Exception {
				return new Tuple2<String, Integer>("total count of distinct senders", arg0.f1);
			}
		});
		
		DataSet<Tuple2<String, Integer>> mailsPerSender = mailCount.union(senderCount).reduce(new ReduceFunction<Tuple2<String,Integer>>() {
			@Override
			public Tuple2<String, Integer> reduce(Tuple2<String, Integer> arg0, Tuple2<String, Integer> arg1) throws Exception {
				
				int mailsPerSender = (arg0.f1 > arg1.f1)? arg0.f1/arg1.f1 : arg1.f1/arg0.f1;
				
				return new Tuple2<String, Integer>("mails per sender", mailsPerSender);
			}
		});
		
		
		
		mailCount.union(senderCount).union(mailsPerSender).print();
		
		
		
		
		/*DataSet<Tuple2<String, String>> mails = mailsTmp.map(new MapFunction<Tuple2<String,String>, Tuple2<String,String>>() {
			@Override
			public Tuple2<String,String> map(Tuple2<String, String> arg) throws Exception {
				int start = arg.f1.indexOf('<');
				int end = arg.f1.indexOf('>');
				return new Tuple2<String, String>(arg.f0.substring(1, 8), arg.f1.substring(start+1, end));
			}
		});
		
		UnsortedGrouping<Tuple2<String,String>> groupedByTime = mails.groupBy(0);
		GroupReduceOperator<Tuple2<String, String>, Tuple3<String,String,Integer>> reducedGroup = groupedByTime
				.sortGroup(1, Order.ASCENDING)
				.reduceGroup(new GroupReduceFunction<Tuple2<String,String>, Tuple3<String,String,Integer>>() {
			@Override
			public void reduce(Iterable<Tuple2<String, String>> arg0, Collector<Tuple3<String, String, Integer>> arg1) throws Exception {
				Iterator<Tuple2<String,String>> iterator = arg0.iterator();
				String currentSender = null;
				int count = 0;
				while(iterator.hasNext()){
					Tuple2<String,String> tuple = iterator.next();
					if(tuple.f1.equals(currentSender)){
						count++;
					} else {
						if(currentSender != null){
							arg1.collect(new Tuple3<String, String, Integer>(tuple.f0, currentSender, count));
						}
						currentSender = tuple.f1;
						count = 1;
					}
				}
			}
		});*/
		
		//DataSet<Tuple3<String,String, Integer>> test = mails.groupBy(0,1).sum(2);
		
		//test.print();
		
		
		// execute program
		//env.execute("Mail per Month count");
	}
}
