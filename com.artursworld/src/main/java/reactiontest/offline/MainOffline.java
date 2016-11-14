package reactiontest.offline;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Offline data analysis
 *
 */
public class MainOffline {
	
	public static void main(String[] args) throws Exception {
		
		// read data using flatMap function
		HumanBenchmark human = new HumanBenchmark();
		human.loadDataSetOfOctober2016();
		
		human.printInputData();
		
		// metric: count using links sum function
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
		double average = human.getAverageReaction();
		System.err.println("October AVG reactionTime = " + average); // 297.26
		
		// metric average for all the data
		human.loadDataSetOfAllTime();
		double averageAll = human.getAverageReaction();
		System.err.println("Alltime AVG reactionTime = " + averageAll); // 283.99
		
		// metric median
		human.loadDataSetOfOctober2016();
		double median = human.getMedianReactionTime();
		System.err.println("October Median reactionTime = " + median); // 285.0
	}
	
}
