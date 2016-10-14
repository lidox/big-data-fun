package batch;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 	0 MessageID  : String // a unique message id
	1 Timestamp  : String // the mail deamon timestamp
	2 Sender     : String // the sender of the mail
	3 Subject    : String // the subject of the mail
	4 Body       : String // the body of the mail (contains linebrakes)
	5 Replied-To : String // the messageID of the mail this mail was replied to 
   
 * @author osboxes
 *
 */
public class MailBatchProcessor {
	
	private ExecutionEnvironment env = null;
	private DataSet<Tuple2<String, String>> mailList = null;
	
	public MailBatchProcessor(String dataPath){
		env = ExecutionEnvironment.getExecutionEnvironment();
		mailList = env.readCsvFile(dataPath)
				.lineDelimiter("##//##")
			    .fieldDelimiter("#|#")
				.includeFields("011")
				.types(String.class, String.class);
	}

	/**
	 * Get something like this:
	 *  (2014-09,fhueske@apache.org,16)
		(2014-09,aljoscha@apache.org,13)
		(2014-09,sewen@apache.org,24)
		(2014-10,fhueske@apache.org,14)
		(2014-10,aljoscha@apache.org,17)
	 * @throws Exception
	 */
	public void getUserAndMailCount() throws Exception{
		mailList.map(new MapFunction<Tuple2<String,String>, Tuple2<String, String>>() {

			@Override
			public Tuple2<String, String> map(Tuple2<String, String> mail) throws Exception {
				// extract year and month from time string
				String month = mail.f0.substring(0, 7);
				// extract email address from the sender
				String email = mail.f1.substring(mail.f1.lastIndexOf("<") + 1, mail.f1.length() - 1);
				// < 2014-09, Anirvan.Basu@alumni.INSEAD.edu >
				return new Tuple2<>(month, email);
			}
			
		})			
		// group by month and email address and count number of records per group
		.groupBy(1, 0).reduceGroup(new GroupReduceFunction<Tuple2<String,String>,  Tuple3<String ,String, Integer>>() {

			@Override
			public void reduce(Iterable<Tuple2<String, String>> mails, Collector<Tuple3<String, String, Integer>> out) throws Exception {

				String month = null;
				String email = null;
				int cnt = 0;

				// count number of tuples
				for(Tuple2<String, String> m : mails) {
					// remember month and email address
					month = m.f0;
					email = m.f1;
					// increase count
					cnt++;
				}

				// emit month, email address, and count
				out.collect(new Tuple3<>(month, email, cnt));
			}


			
		})
		// print the result
		.print();
	}
}

