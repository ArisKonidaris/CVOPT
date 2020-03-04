package operators.Job1;

import dataStructures.DataTuple;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import state.StatsAccumulator;
import state.StatsAggregate;

/**
 * A KeyedProcessFunction Operator that calculates the statistics for each stratum.
 * This Operator uses a timer to indicate the maximum waiting period for a group by
 * record to arrive. When the timer fires, the resulting statistics are emitted.
 */
public class StatisticsCalculation
        extends KeyedProcessFunction<String, DataTuple, Tuple4<String, Double, Double, Integer>> {

    private Long timeout; // The maximum waiting period
    private ValueState<Long> timestampState; // The latest timestamp for each stratum
    private ValueState<Boolean> flag; // This flag is responsible for collecting the statistics of each stratum
    private AggregatingState<Double, Tuple3<Integer, Double, Double>> stats; // The statistics each stratum
                                                                             // Mean, Standard Deviation, Size

    public StatisticsCalculation(Long timeout) {
        this.timeout = timeout;
    }

    /**
     * @param dataTuple An input data tuple or a polling message
     * @param collector The resulting statistics for each stratum (Tuple4 -> Stratum, Mean, Standard Deviation, Size)
     */
    @Override
    public void processElement(DataTuple dataTuple,
                               Context context,
                               Collector<Tuple4<String, Double, Double, Integer>> collector) throws Exception {

        // Sends a pseudo result to the next operator to sort the resulting strata according to the group by attributes
        if (stats.get() == null)
            collector.collect(new Tuple4<>(dataTuple.groupBy_attributes, 0.0, 0.0, 0));

        // If the tuple is not a polling tuple, then update the statistics of the stratum
        if (!dataTuple.isPoll()) stats.add(dataTuple.aggregate_attribute);

        // Set the state's timestamp to the record's assigned timestamp
        Long tempTime = context.timestamp();
        timestampState.update(tempTime);

        // Schedule the next timer timeout ms from the current record time
        context.timerService().registerEventTimeTimer(tempTime + timeout);
    }

    @Override
    public void open(Configuration parameters) {
        timestampState = getRuntimeContext().getState(new ValueStateDescriptor<>("timestampState", Long.class));
        flag = getRuntimeContext().getState(new ValueStateDescriptor<>("flag", Boolean.class, false));
        stats = getRuntimeContext().getAggregatingState(
                new AggregatingStateDescriptor<>(
                        "stats",
                        new StatsAccumulator(),
                        TypeInformation.of(new TypeHint<StatsAggregate>() {
                        })
                )
        );
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple4<String, Double, Double, Integer>> out) throws Exception {

        // Get the state for the key that scheduled the timer
        Long stateTime = timestampState.value();

        // Check if this is an outdated timer or the latest timer
        if (timestamp == stateTime + timeout) {
            // Emit the state on timeout
            if (!flag.value()) {
                out.collect(new Tuple4<>(ctx.getCurrentKey(), stats.get().f1, stats.get().f2, stats.get().f0));
                flag.update(true);
            }
        }
    }


}