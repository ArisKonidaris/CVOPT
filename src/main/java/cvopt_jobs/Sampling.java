package cvopt_jobs;

import dataStructures.DataTuple;
import dataStructures.KafkaRecord;
import dataStructures.MySchema;
import operators.FlatMapToKey;
import operators.Job2.ProcessToSI;
import operators.Job2.Reservoir;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

/**
 * <br>TECHNICAL UNIVERSITY OF CRETE
 * <br>SCHOOL OF ELECTRICAL & COMPUTER ENGINEERING
 * <br>Master course: ECE622
 * <br>First phase of CVOPT_SASG algorithm
 * <br>Authors: Konidaris Vissarion, Bitsakis Theodoros
 * <br>email: v.b.konidaris@gmail.com, t4bitsakis@gmail.com
 */
public class Sampling {

    /**
     * This is the second phase of the CVOPT algorithm 1.
     * The job selects with reservoir sampling a random sample
     * from the input dataset for each stratum.
     */
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        Random random = new Random(System.currentTimeMillis());
        String kafka_data_topic = params.get("kafka_data_topic", "OpenAQ");
        String kafka_statistics_topic = params.get("kafka_statistics_topic", "Statistics");
        String kafka_broker_list = params.get("kafka_broker_list", "localhost:9092");
        String parallelism = params.get("parallelism", "4");
        String groupBys = params.get("groupBy_columns", "3,6,8");
        Integer[] groupBy_columns = Arrays.stream(groupBys.split(",")).map(Integer::parseInt).toArray(Integer[]::new);
        int aggregate_column = Integer.parseInt(params.get("aggregate_column", "7"));
        Integer memory_budget = Integer.parseInt(params.get("memory_budget", "150"));
        String weight_vector = params.get("w", "1,1,1,1,1,1,1,1,1,1,1,1,1,1,1");
        ArrayList<Double> w = new ArrayList<>(
                Arrays.asList(
                        Arrays.stream(weight_vector.split(",")).map(Double::parseDouble).toArray(Double[]::new)
                )
        );
        for (int index = 0; index < w.size(); index++) if (w.get(index) < 0.0) w.set(index, 1.0);
        Long timeout = Long.parseLong(params.get("timeout", "1000"));

        System.out.println("\nFlink Job for sampling each group by, using the CVOPT algorithm");
        System.out.println("1.kafka data topic: " + kafka_data_topic);
        System.out.println("2.kafka data consumer group.id: " + kafka_data_topic + "_consumer");
        System.out.println("3.kafka statistics topic: " + kafka_statistics_topic);
        System.out.println("4.kafka statistics consumer group.id: " + kafka_statistics_topic + "_consumer");
        System.out.println("5.kafka broker list: " + kafka_broker_list);
        System.out.println("6.parallelism: " + parallelism);
        System.out.println("7.group by columns: " + groupBys);
        System.out.println("8.aggregate column: " + aggregate_column);
        System.out.println("9.memory budget: " + memory_budget + "\n");

        // 1. Obtain Execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.setParallelism(Integer.parseInt(parallelism));
        env.getConfig().setGlobalJobParameters(params);
        env.registerType(KafkaRecord.class);

        // 2. Data Sources
        final Properties properties_data = new Properties();
        properties_data.setProperty("bootstrap.servers", kafka_broker_list);
        properties_data.setProperty("group.id", kafka_data_topic + "_consumer");
        DataStream<KafkaRecord> data_stream = env
                .addSource(new FlinkKafkaConsumer<>(kafka_data_topic, new MySchema(), properties_data)
                        .setStartFromEarliest())
                .returns(KafkaRecord.class)
                .name("kafka_data_consumer");

        final Properties properties_stats = new Properties();
        properties_stats.setProperty("bootstrap.servers", kafka_broker_list);
        properties_stats.setProperty("group.id", kafka_statistics_topic + "_consumer");
        DataStream<Tuple4<String, Double, Double, Integer>> statistics_stream = env
                .addSource(new FlinkKafkaConsumer<>(
                        kafka_statistics_topic,
                        new TypeInformationSerializationSchema<>(
                                TypeInformation.of(new TypeHint<Tuple4<String, Double, Double, Integer>>() {
                                }),
                                env.getConfig()
                        ),
                        properties_stats)
                        .setStartFromEarliest())
                .name("kafka_statistics_consumer");

        // 3. Transformations
        DataStream<DataTuple> tuples_stream = data_stream
                .flatMap(new FlatMapToKey(groupBy_columns, aggregate_column, timeout))
                .name("flat_map_to_key");

        DataStream<DataTuple> si_stream = statistics_stream
                .keyBy(x -> 0)
                .process(new ProcessToSI(memory_budget, w))
                .setParallelism(1)
                .name("process_to_si");

        DataStream<DataTuple> sample_stream = si_stream
                .union(tuples_stream)
                .keyBy(x -> x.groupBy_attributes)
                .process(new Reservoir(random, timeout))
                .name("keyed_process_to_sampling");


        // 4. Data Sink
        sample_stream.print();

        // 5. Execute streaming job
        env.execute("Phase2: Reservoir Sampling");

    }
}
