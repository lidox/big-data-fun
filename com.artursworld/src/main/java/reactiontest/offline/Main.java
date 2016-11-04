package reactiontest.offline;

/**
 * Offline data analysis
 *
 */
public class Main {
	
	public static void main(String[] args) {
		HumanBenchmark human = new HumanBenchmark();
		//human.printInputData();
		System.out.println("1. RT Count: "+human.getReactionTestCount()); // 1368464
		System.out.println("2. RT Count: "+human.getReactionTestCountByReduce()); // 1368464
	}
	
}
