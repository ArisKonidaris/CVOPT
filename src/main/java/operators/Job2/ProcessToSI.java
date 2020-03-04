package operators.Job2;

import dataStructures.DataTuple;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * This KeyedProcessFunction Operator calculates the memory distribution of each stratum (S_i = M * gamma_i/gamma)
 */
public class ProcessToSI
        extends KeyedProcessFunction<Integer, Tuple4<String, Double, Double, Integer>, DataTuple> {

    private ArrayList<Stats> stratumInfo; // Stores the information of each stratum
    private int memoryBudget; // The total memory budget M provided by the user
    private ArrayList<Double> weight_vector; // The weights w for each stratum provided by the user
    private transient ValueState<Long> tupleTimestamp; // The latest timestamp
    private boolean flag; // When true, this operator emits the memory budget of each stratum
    private Long number_of_strata;

    public ProcessToSI(Integer memoryBudget, ArrayList<Double> weight_vector) {
        stratumInfo = new ArrayList<>();
        this.memoryBudget = memoryBudget;
        this.weight_vector = weight_vector;
        number_of_strata = 0L;
    }

    @Override
    public void processElement(Tuple4<String, Double, Double, Integer> stats,
                               Context context,
                               Collector<DataTuple> collector) throws Exception {


        // Initialize flag variable
        if (stratumInfo.isEmpty()) flag = false;

        // Store the statistics of each stratum, provided by the first Flink job
        stratumInfo.add(new Stats(stats.f0, stats.f1, stats.f2, stats.f3));

        // Count the number of strata
        number_of_strata++;

        // Set the state's timestamp to the record's assigned time timestamp
        Long tempTime = context.timestamp();
        tupleTimestamp.update(tempTime);

        // Schedule the next timer timeout ms from the current record time
        context.timerService().registerEventTimeTimer(tempTime + 200);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<DataTuple> out) throws Exception {

        // check if this is an outdated timer or the latest timer
        if (timestamp == tupleTimestamp.value() + 200) {

            // emit the state on timeout
            if (!flag) {

                // Calculate the size of the entire data set
                // This is done to check if the Memory Budget is greater than the size of the input dataset
                int total_count = 0;
                for (Stats stat : stratumInfo)
                    total_count += stat.count;
                if (total_count < memoryBudget)
                    throw new RuntimeException("The memory budget is larger than the actual size of the dataset");

                // Print the number of stratums
                System.out.println("\nNumber of strata/group bys: " + number_of_strata + "\n");

                // 1. Apply the weight vector and calculate total gamma
                double total_gamma = 0.0;
                for (int index = 0; index < stratumInfo.size(); index++) {
                    double CV = stratumInfo.get(index).stratum_std / stratumInfo.get(index).stratum_mean;
                    double Wsqrt;
                    if (weight_vector.size() == stratumInfo.size())
                        Wsqrt = Math.sqrt(weight_vector.get(index));
                    else
                        Wsqrt = 1.0;
                    stratumInfo.get(index).gamma = Wsqrt * CV;
                    total_gamma += stratumInfo.get(index).gamma;
                }

                // 2. Calculate gamma_i/gamma
                for (Stats stat : stratumInfo)
                    stat.gamma /= total_gamma;

                // 3. Calculate the memory budget distribution
                for (Stats stat : stratumInfo) {
                    double mem_i = ((double) memoryBudget) * stat.gamma;
                    if (mem_i < 1.0) {
                        stat.si = 1.0;
                        stat.si_floor = 1;
                    } else {
                        stat.si = mem_i;
                        stat.si_floor = (int) Math.floor(mem_i);
                    }
                }

                int totalMem = 0;
                for (Stats stat : stratumInfo) {
                    if (stat.si_floor > stat.count)
                        stat.si_floor = stat.count;
                    totalMem += stat.si_floor;
                }

                int memDiff = memoryBudget - totalMem;

                System.out.println("Total Memory: " + totalMem);
                System.out.println("Memory Budget: " + memoryBudget);
                System.out.println("Memory Difference: " + memDiff);

                // 4. Sort si based on its decimal part
                stratumInfo.sort(Comparator.comparing(s -> (s.si - ((double) s.si_floor))));

                // 5. Redistribute the memory difference using the Largest Remainder Method
                if (memDiff > 0) {
                    int index = 0;
                    Collections.reverse(stratumInfo);
                    while (memDiff != 0) {
                        if (index == stratumInfo.size()) index = 0;
                        if (stratumInfo.get(index).si_floor < stratumInfo.get(index).count) {
                            stratumInfo.get(index).si_floor += 1;
                            memDiff--;
                        }
                        index++;
                    }
                } else if (memDiff < 0) {
                    int index = 0;
                    while (memDiff != 0) {
                        if (index == stratumInfo.size()) index = 0;
                        if (stratumInfo.get(index).si_floor > 1) {
                            stratumInfo.get(index).si_floor -= 1;
                            memDiff++;
                        }
                        index++;
                    }
                }


                // Output the results
                for (Stats stratum : stratumInfo) {
                    DataTuple result = new DataTuple(stratum.groupBy_attributes, true, stratum.si_floor);
                    result.setStratum_mean(stratum.stratum_mean);
                    result.setStratum_std(stratum.stratum_std);
                    result.setCount(stratum.count);
                    out.collect(result);
                }
                flag = true;
            }

        }
    }

    @Override
    public void open(Configuration parameters) {
        tupleTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("tupleTimestamp", Long.class));
    }

    /**
     * A helper inner static class used for calculating the s_i
     * It stores all the necessary information that CVOPT uses
     * to calculate the memory distribution of each stratum
     */
    public static class Stats {
        public String groupBy_attributes;
        public Double stratum_mean;
        public Double stratum_std;
        public Integer count;
        public Double gamma;
        public Double si;
        public Integer si_floor;

        public Stats(String groupBy_attributes, Double stratum_mean, Double stratum_std, Integer count) {
            this.groupBy_attributes = groupBy_attributes;
            this.stratum_mean = stratum_mean;
            this.stratum_std = stratum_std;
            this.count = count;
        }
    }

}

