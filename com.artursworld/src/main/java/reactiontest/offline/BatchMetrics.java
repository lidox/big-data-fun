package reactiontest.offline;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

public class BatchMetrics {

	public static void main(String[] args) {

		try {

			// read data using flatMap function
			BatchFunctions batch = new BatchFunctions();
			DataSet<Tuple2<Double, Integer>> dataSet1 = batch.loadDataSetOfOctober2016();

			batch.printInputData(dataSet1);

			// metric: count using links sum function
			System.err.println("1. RT Count: "+ batch.getReactionTestCountBySum(dataSet1));

			// metric: count using flinks reduce function
			System.err.println("2. RT Count: "
					+ batch.getReactionTestCountByReduce(dataSet1));

			// metric: minimum
			Tuple2<Double, Integer> recordWithMinUserCount = batch
					.getReactionTimeByMinUserCount(dataSet1);
			System.err.println("Min user count = " + recordWithMinUserCount.f1
					+ " with RT = " + recordWithMinUserCount.f0);

			// metric maximum
			Tuple2<Double, Integer> recordWithMaxUserCount = batch
					.getReactionTimeByMaxUserCount(dataSet1);
			System.err.println("Max user count = " + recordWithMaxUserCount.f1
					+ " with RT = " + recordWithMaxUserCount.f0);

			// metric minimum / maximum using aggregate function
			Tuple2<Double, Integer> maxAggregate = batch
					.getMaxUserCountByAggregate(dataSet1);
			System.err.println("Max user count = " + maxAggregate.f1
					+ " with RT = " + maxAggregate.f0);

			// metric average
			double average = batch.getAverageReaction(dataSet1);
			System.err.println("October AVG reactionTime = " + average); // 297.26

			// metric average for all the data
			DataSet<Tuple2<Double, Integer>> dataSet2 = batch.loadDataSetOfAllTime();
			double averageAll = batch.getAverageReaction(dataSet2);
			System.err.println("Alltime AVG reactionTime = " + averageAll); // 283.99

			// metric median
			DataSet<Tuple2<Double, Integer>> dataSet3 = batch.loadDataSetOfOctober2016();
			double median = batch.getMedianReactionTime(dataSet3);
			System.err.println("October Median reactionTime = " + median); // 285.0

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
