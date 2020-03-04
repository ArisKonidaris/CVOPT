package state;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * This class is an Integer Flink accumulator,
 * used as a counter by the reservoir sampling algorithms.
 */
public class IntegerAccumulator implements AggregateFunction<Integer, IntegerAggregate, Integer> {
    @Override
    public IntegerAggregate createAccumulator() {
        return new IntegerAggregate();
    }

    @Override
    public IntegerAggregate add(Integer integer, IntegerAggregate integerAggregate) {
        integerAggregate.setValue(integer + integerAggregate.getValue());
        return integerAggregate;
    }

    @Override
    public Integer getResult(IntegerAggregate integerAggregate) {
        return integerAggregate.getValue();
    }

    @Override
    public IntegerAggregate merge(IntegerAggregate integerAggregate, IntegerAggregate acc1) {
        return new IntegerAggregate(integerAggregate.getValue() + acc1.getValue());
    }
}
