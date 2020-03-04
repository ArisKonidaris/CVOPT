package operators.Job1;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * This operator gathers the results of the First Job (the statistics of each stratum) and
 * sorts them according to their group by attributes. The sorted results are emitted only once.
 */
public class SortStatistics
        extends RichFlatMapFunction<Tuple4<String, Double, Double, Integer>,
        Tuple4<String, Double, Double, Integer>> implements CheckpointedFunction {

    private Map<String, Tuple4<String, Double, Double, Integer>> results; // The sorted resulting stratum statistics
    private transient ListState<Map<String, Tuple4<String, Double, Double, Integer>>> checkpointed_results;

    public SortStatistics() {
        results = new HashMap<>();
    }

    @Override
    public void flatMap(Tuple4<String, Double, Double, Integer> input,
                        Collector<Tuple4<String, Double, Double, Integer>> collector) {

        // Collect the pseudo or real results for each stratum
        results.put(input.f0, input);

        // If no pseudo results remain in the map, emit the sorted stratum statistics
        for (Tuple4<String, Double, Double, Integer> stratum : results.values())
            if (stratum.f3 == 0) return;

        // Sort the stratum statistics and emit them
        ArrayList<Tuple4<String, Double, Double, Integer>> output = new ArrayList<>(results.values());
        output.sort(Comparator.comparing(s -> s.f0));
        for (Tuple4<String, Double, Double, Integer> result : output)
            collector.collect(result);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointed_results.clear();
        checkpointed_results.add(results);
    }

    @Override
    public void initializeState(FunctionInitializationContext fctx) throws Exception {
        checkpointed_results = fctx.getOperatorStateStore().getListState(
                new ListStateDescriptor<>(
                        "checkpointed_results",
                        TypeInformation.of(new TypeHint<Map<String, Tuple4<String, Double, Double, Integer>>>() {
                        })
                )
        );
    }
}
