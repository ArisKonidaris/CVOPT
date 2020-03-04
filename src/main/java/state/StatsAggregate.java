package state;

import java.io.Serializable;

/**
 * This is a class that contains the aggregated statistics for a each stratum,
 * used by the {@link StatsAccumulator}.
 * (count, mean , variance)
 */
public class StatsAggregate implements Serializable {
    public Integer count;
    public Double mean;
    public Double d_squared;

    public StatsAggregate() {
        this(0, 0.0, 0.0);
    }

    public StatsAggregate(Integer count, Double mean, Double d_squared) {
        this.count = count;
        this.mean = mean;
        this.d_squared = d_squared;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Double getMean() {
        return mean;
    }

    public void setMean(Double mean) {
        this.mean = mean;
    }

    public Double getDSquared() {
        return d_squared;
    }

    public void setDSquared(Double d_squared) {
        this.d_squared = d_squared;
    }
}
