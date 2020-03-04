package state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * This class is a Flink accumulator that calculates incrementally the
 * mean, standard deviation and the count of each stratum by using {@link StatsAggregate}
 */
public class StatsAccumulator implements AggregateFunction<Double, StatsAggregate, Tuple3<Integer, Double, Double>> {

    @Override
    public StatsAggregate createAccumulator() {
        return new StatsAggregate();
    }

    @Override
    public StatsAggregate add(Double aDouble, StatsAggregate stats) {
        stats.setCount(stats.getCount() + 1);
        Double newMean = stats.getMean() + (aDouble - stats.getMean()) / (1.0 * stats.getCount());
        stats.setDSquared(stats.getDSquared() + (aDouble - newMean) * (aDouble - stats.getMean()));
        stats.setMean(newMean);
        return stats;
    }

    @Override
    public Tuple3<Integer, Double, Double> getResult(StatsAggregate stats) {
        return new Tuple3<>(stats.count, stats.getMean(), Math.sqrt(stats.getDSquared() / (1.0 * (stats.getCount()))));
    }

    @Override
    public StatsAggregate merge(StatsAggregate ag1, StatsAggregate ag2) {
        Integer n1 = ag1.getCount();
        Integer n2 = ag2.getCount();
        Double m1 = ag1.getMean();
        Double m2 = ag2.getMean();
        double v1 = ag1.getDSquared() / (1.0 * n1);
        double v2 = ag2.getDSquared() / (1.0 * n2);
        Double m = (m1 * n1 + m2 * n2) / (1.0 * (n1 + n2));
        Double mergedDSSquared = (n1 * (v1 + Math.pow(m1 - m, 2)) + n2 * (v2 + Math.pow(m2 - m, 2)));
        return new StatsAggregate(n1 + n2, m, mergedDSSquared);
    }
}