package state;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * This class is a FIFO Flink accumulator, used as a tuple buffer
 * by the reservoir sampling algorithms.
 *
 * @param <T> The data tuple
 */
public class FifoAccumulator<T> implements AggregateFunction<T, FifoAggregate<T>, T> {
    @Override
    public FifoAggregate<T> createAccumulator() {
        return new FifoAggregate<>();
    }

    @Override
    public FifoAggregate<T> add(T element, FifoAggregate<T> tFifoAggregate) {
        tFifoAggregate.push(element);
        return tFifoAggregate;
    }

    @Override
    public T getResult(FifoAggregate<T> tFifoAggregate) {
        return tFifoAggregate.pop();
    }

    @Override
    public FifoAggregate<T> merge(FifoAggregate<T> fifo1, FifoAggregate<T> fifo2) {
        while (!fifo2.isEmpty()) fifo1.push(fifo2.pop());
        return fifo2;
    }
}
