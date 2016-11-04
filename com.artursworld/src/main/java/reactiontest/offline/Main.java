package reactiontest.offline;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Offline data analysis
 *
 */
public class Main {
	
	public static void main(String[] args) {
		HumanBenchmark human = new HumanBenchmark();
		human.printInputData();
		
		// metric: count
		System.out.println("1. RT Count: "+human.getReactionTestCountBySum()); // 1368464
		System.out.println("2. RT Count: "+human.getReactionTestCountByReduce()); // 1368464
		
		// metric: minimum
		Tuple2<Double, Integer> recordWithMinUserCount = human.getReactionTimeByMinUserCount();
		System.out.println("Min user count = "+recordWithMinUserCount.f1+" with RT = "+recordWithMinUserCount.f0);
		
		// metric maximum
		Tuple2<Double, Integer> recordWithMaxUserCount = human.getReactionTimeByMaxUserCount();
		System.out.println("Max user count = "+recordWithMaxUserCount.f1+" with RT = "+recordWithMaxUserCount.f0);
	}
	
}
