package operators.Job2;

import dataStructures.DataTuple;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import state.FifoAccumulator;
import state.FifoAggregate;
import state.IntegerAccumulator;
import state.IntegerAggregate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

/**
 * A KeyedProcessFunction Operator that samples records from each stratum
 * using the OptimalReservoir sampling algorithm. This Operator uses a timer to
 * indicate the maximum waiting period for a group by record to arrive.
 * When the timer fires, the resulting samples are emitted.
 */
public class OptimalReservoir extends KeyedProcessFunction<String, DataTuple, DataTuple> {

    private ValueState<Boolean> flag; // This flag is responsible for collecting the samples of each stratum
    private ValueState<Long> timestampState; // The latest timestamp for each stratum
    private ValueState<DataTuple> stats_tuple; // The info of each stratum
    private ValueState<Double> W; // The W variable used by the optimal reservoir sampling algorithm
    private ValueState<Long> skip; // How many records to skip before inserting a record in the sample
    private AggregatingState<Integer, Integer> count; // The counter for the reservoir sampling algorithm
    private MapState<Integer, DataTuple> sample; // The sample of each stratum

    // The Queue that buffers tuples until the memory budget of the stratum arrives
    private AggregatingState<DataTuple, DataTuple> buffer;

    private Long timeout; // The maximum waiting period
    private Random random; // A random number generator

    public OptimalReservoir(Random random, Long timeout) {
        this.random = random;
        this.timeout = timeout;
    }

    @Override
    public void processElement(DataTuple dataTuple,
                               Context context,
                               Collector<DataTuple> collector) throws Exception {

        // Buffering and sampling
        if (stats_tuple.value() == null) {
            if (dataTuple.isStats()) {
                stats_tuple.update(dataTuple);
                W.update(Math.pow(randomExclusive(), 1.0 / dataTuple.getSi()));
                skip.update(skipLength());
                int counter = 0;
                while (counter < dataTuple.si) {
                    try {
                        DataTuple tuple = buffer.get();
                        if (tuple == null) break;
                        sample.put(++counter, tuple);
                    } catch (Exception e) {
                        break;
                    }
                }
                count.add(counter);
                if (counter == dataTuple.si) {
                    while (true) {
                        try {
                            DataTuple tuple = buffer.get();
                            if (tuple == null) break;
                            OptimalReservoirSampling(tuple);
                        } catch (Exception e) {
                            break;
                        }
                    }
                }
            } else if (!dataTuple.isPoll()) buffer.add(dataTuple);
        } else if (!dataTuple.isPoll()) OptimalReservoirSampling(dataTuple);


        if (!dataTuple.isStats()) {
            // Set the state's timestamp to the record's assigned timestamp
            Long tempTime = context.timestamp();
            timestampState.update(tempTime);

            // Schedule the next timer timeout ms from the current record time
            context.timerService().registerEventTimeTimer(tempTime + timeout);
        }

    }

    @Override
    public void open(Configuration parameters) {
        timestampState = getRuntimeContext().getState(new ValueStateDescriptor<>("timestampState", Long.class));
        flag = getRuntimeContext().getState(new ValueStateDescriptor<>("flag", Boolean.class, false));
        stats_tuple = getRuntimeContext().getState(new ValueStateDescriptor<>("stats_tuple", DataTuple.class));
        sample = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("sample", Integer.class, DataTuple.class)
        );
        buffer = getRuntimeContext().getAggregatingState(
                new AggregatingStateDescriptor<>(
                        "buffer",
                        new FifoAccumulator<>(),
                        TypeInformation.of(new TypeHint<FifoAggregate<DataTuple>>() {
                        })
                )
        );
        count = getRuntimeContext().getAggregatingState(
                new AggregatingStateDescriptor<>(
                        "count",
                        new IntegerAccumulator(),
                        TypeInformation.of(new TypeHint<IntegerAggregate>() {
                        })
                )
        );
        W = getRuntimeContext().getState(new ValueStateDescriptor<>("W", Double.class));
        skip = getRuntimeContext().getState(new ValueStateDescriptor<>("skip", Long.class));
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<DataTuple> out) throws Exception {

        // Get the state for the key that scheduled the timer
        Long stateTime = timestampState.value();

        // Check if this is an outdated timer or the latest timer
        if (timestamp == stateTime + timeout) {

            // Emit the state on timeout
            if (!flag.value() && stats_tuple.value() != null) {

                // Calculating the statistics of the sample
                double sample_mean = 0.0;
                double sample_std = 0.0;
                int counter = 0;
                DataTuple stats = stats_tuple.value();

                ArrayList<DataTuple> sample_of_strata = new ArrayList<>();
                for (DataTuple tuple : sample.values()) {
                    counter++;
                    sample_mean += (tuple.aggregate_attribute - sample_mean) / (1.0 * counter);
                    sample_of_strata.add(tuple);
                }

                if (sample_of_strata.size() > 1) {
                    for (DataTuple tuple : sample_of_strata)
                        sample_std += Math.pow(tuple.aggregate_attribute - sample_mean, 2);
                    sample_std /= sample_of_strata.size();
                    sample_std = Math.sqrt(sample_std);
                }

//                if (stats_tuple.value().getSi() != counter) System.out.println("------------------->");

                DataTuple result = new DataTuple(stats.groupBy_attributes, false, stats.si);
                result.setStratum_mean(stats.getStratum_mean());
                result.setStratum_std(stats.getStratum_std());
                result.setCount(stats.getCount());
                result.setSample_mean(sample_mean);
                result.setSample_std(sample_std);
                result.setStratum_sample(sample_of_strata);

                out.collect(result);
                flag.update(true);
                sample.clear();
            }
        }
    }

    // Returns a pseudorandom double value in (0,1) exclusive
    public double randomExclusive() {
        double r = 0.0;
        while (r == 0.0) {
            r = random.nextDouble();
        }
        assert r > 0.0 && r < 1.0;
        return r;
    }

    public long skipLength() throws IOException {
        final double random1 = randomExclusive();
        final double random2 = randomExclusive();
        long skip = (long) (Math.log(random1) / Math.log(1 - W.value()));
        if (skip == Long.MIN_VALUE) skip = Long.MAX_VALUE;
        W.update(W.value() * Math.pow(random2, 1.0 / stats_tuple.value().si));
        return skip;
    }

    /**
     * This method implements the optimal reservoir sampling algorithm
     */
    public void OptimalReservoirSampling(DataTuple dataTuple) throws Exception {
        int counter = count.get() + 1;
        if (counter <= stats_tuple.value().si)
            sample.put(counter, dataTuple);
        else {
            if (skip.value() > 0)
                skip.update(skip.value() - 1);
            else {
                assert skip.value() == 0;
                sample.put(random.nextInt(stats_tuple.value().si) + 1, dataTuple);
                skip.update(skipLength());
                assert skip.value() >= 0;
            }
        }
        count.add(1);
    }


}