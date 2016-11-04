package reactiontest.offline;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Offline data analysis
 *
 */
public class Main {
	
	public static void main(String[] args) {
		
		// read data using flatMap function
		HumanBenchmark human = new HumanBenchmark();
		human.printInputData();
		
		// metric: count using flinks sum function
		System.err.println("1. RT Count: "+human.getReactionTestCountBySum()); 
		
		// metric: count using flinks reduce function
		System.err.println("2. RT Count: "+human.getReactionTestCountByReduce()); 
		
		// metric: minimum 
		Tuple2<Double, Integer> recordWithMinUserCount = human.getReactionTimeByMinUserCount();
		System.err.println("Min user count = "+recordWithMinUserCount.f1+" with RT = "+recordWithMinUserCount.f0);
		
		// metric maximum
		Tuple2<Double, Integer> recordWithMaxUserCount = human.getReactionTimeByMaxUserCount();
		System.err.println("Max user count = "+recordWithMaxUserCount.f1+" with RT = "+recordWithMaxUserCount.f0);
		
		// metric minimum / maximum using aggregate function
		Tuple2<Double, Integer> maxAggregate = human.getMaxUserCountByAggregate();
		System.err.println("Max user count = "+maxAggregate.f1+" with RT = "+maxAggregate.f0);
		
		// metric average
		//TODO:
		
		// metric median
		//TODO: 
	}
	
}
