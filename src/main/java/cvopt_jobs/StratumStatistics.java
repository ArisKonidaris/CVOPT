package cvopt_jobs;

import dataStructures.DataTuple;
import dataStructures.KafkaRecord;
import dataStructures.MySchema;
import operators.FlatMapToKey;
import operators.Job1.SortStatistics;
import operators.Job1.StatisticsCalculation;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Arrays;
import java.util.Properties;

/**
 * <br>TECHNICAL UNIVERSITY OF CRETE
 * <br>SCHOOL OF ELECTRICAL & COMPUTER ENGINEERING
 * <br>Master course: ECE622
 * <br>First phase of CVOPT_SASG algorithm
 * <br>Authors: Bitsakis Theodoros, Konidaris Vissarion
 * <br>email: v.b.konidaris@gmail.com, t4bitsakis@gmail.com
 */
public class StratumStatistics {

    /**
     * This is the first phase of the CVOPT algorithm 1.
     * The job calculates the statistics for each stratum. (mean, standard deviation, size)
     */
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        String kafka_data_topic = params.get("kafka_data_topic", "OpenAQ");
        String kafka_broker_list = params.get("kafka_broker_list", "localhost:9092");
        String parallelism = params.get("parallelism", "4");
        String groupBys = params.get("groupBy_columns", "3,6,8");
        Integer[] groupBy_columns = Arrays.stream(groupBys.split(",")).map(Integer::parseInt).toArray(Integer[]::new);
        Integer aggregate_column = Integer.parseInt(params.get("aggregate_column", "7"));
        String kafka_sink_topic = params.get("kafka_sink_topic", "Statistics");
        Long timeout = Long.parseLong(params.get("timeout", "1000"));

        System.out.println("\nFlink Job for calculating the mean and the standard deviation of each group by");
        System.out.println("1.kafka data topic: " + kafka_data_topic);
        System.out.println("2.kafka data consumer group.id: " + kafka_data_topic + "_consumer");
        System.out.println("3.kafka sink topic: " + kafka_sink_topic);
        System.out.println("4.kafka broker list: " + kafka_broker_list);
        System.out.println("5.parallelism: " + parallelism);
        System.out.println("6.group by columns: " + groupBys);
        System.out.println("7.aggregate column: " + aggregate_column + "\n");

        // 1. Obtain Execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.setParallelism(Integer.parseInt(parallelism));
        env.getConfig().setGlobalJobParameters(params);
        env.registerType(KafkaRecord.class);

        // 2. Data Source
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafka_broker_list);
        properties.setProperty("group.id", kafka_data_topic + "_consumer");

        DataStream<KafkaRecord> input_stream = env
                .addSource(new FlinkKafkaConsumer<>(kafka_data_topic, new MySchema(), properties)
                        .setStartFromEarliest())
                .returns(KafkaRecord.class)
                .name("kafka_consumer");

        // 3. Transformations
        DataStream<DataTuple> tuples_stream = input_stream
                .flatMap(new FlatMapToKey(groupBy_columns, aggregate_column, timeout))
                .name("flat_map_to_key");

        DataStream<Tuple4<String, Double, Double, Integer>> statistics_stream = tuples_stream
                .keyBy(x -> x.groupBy_attributes)
                .process(new StatisticsCalculation(timeout))
                .name("keyed_process_to_statistics");

        DataStream<Tuple4<String, Double, Double, Integer>> results = statistics_stream
                .keyBy(x -> 0)
                .flatMap(new SortStatistics())
                .name("sort_statistics");

        // 4. Data Sink
        results.print();
        results
                .addSink(
                        new FlinkKafkaProducer<>(kafka_broker_list,
                                kafka_sink_topic,
                                new TypeInformationSerializationSchema<>(
                                        TypeInformation.of(new TypeHint<Tuple4<String, Double, Double, Integer>>() {
                                        }),
                                        env.getConfig()
                                )
                        )
                )
                .name("kafka_sink");

        // 5. Execute streaming job
        env.execute("Phase1: Stratum Statistics");
    }
}