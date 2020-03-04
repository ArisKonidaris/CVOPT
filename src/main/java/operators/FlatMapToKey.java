package operators;

import dataStructures.DataTuple;
import dataStructures.KafkaRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * This is a RichFlatMapFunction Operator which consumers the input data points
 * and transforms them into a {@link DataTuple} by using a CSV parser. This Operator
 * is also responsible for generating polling flags to keep the key by timers alive.
 */
public class FlatMapToKey
        extends RichFlatMapFunction<KafkaRecord, DataTuple>
        implements CheckpointedFunction {

    private Long timeout;
    private Map<String, Long> key_timers;
    private ListState<Map<String, Long>> s_key_timers; // Timers for each group by
    private Integer[] groupBy_columns; // e.g. 1,2,3,4 starting from [1... to number of columns]
    private Integer aggregate_column; // e.g. 5 starting from [1... to number of columns]

    public FlatMapToKey(Integer[] groupBy_columns, Integer aggregate_column, Long timeout) {
        this.timeout = timeout;
        this.groupBy_columns = groupBy_columns;
        this.aggregate_column = aggregate_column;
        this.key_timers = new HashMap<>();
    }

    @Override
    public void flatMap(KafkaRecord s, Collector<DataTuple> collector) throws Exception {

        // Parse the value of the Kafka record that is written in csv format
        CSVRecord record = CSVParser.parse(s.value, CSVFormat.RFC4180.withIgnoreSurroundingSpaces()).getRecords().get(0);

        // Parse the key/strata/groupBy by concatenating the attribute columns
        StringBuilder groupBy_key = new StringBuilder();
        for (Integer index : groupBy_columns) {
            if (record.get(index - 1).equals("")) return;
            groupBy_key.append(",").append(record.get(index - 1));
        }

        // Parse the aggregate column
        String aggregate_str = record.get(aggregate_column - 1);
        if (aggregate_str.equals("")) return;
        String groupBy = groupBy_key.toString().substring(1);
        double aggregate_attribute = Double.parseDouble(aggregate_str);

        // Poll a group by to keep it alive until the end of the entire data stream
        Long current_time = System.currentTimeMillis();
        key_timers.put(groupBy, System.currentTimeMillis());
        for (Map.Entry<String, Long> entry : key_timers.entrySet())
            if (!entry.getKey().equals(groupBy)) {
                if (current_time - entry.getValue() > timeout * 0.5) {
                    collector.collect(new DataTuple(entry.getKey()));
                    entry.setValue(current_time);
                }
            }

        collector.collect(new DataTuple(groupBy, aggregate_attribute));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        s_key_timers.clear();
        s_key_timers.add(key_timers);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        s_key_timers = functionInitializationContext.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("s_key_timers",
                        TypeInformation.of(new TypeHint<Map<String, Long>>() {
                        })
                )
        );
    }
}