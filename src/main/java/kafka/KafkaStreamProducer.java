package kafka;

import cvopt_jobs.StratumStatistics;
import org.apache.commons.lang3.SystemUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * This is the basic Kafka Producer that is used by the project to feed the data set
 * to a Kafka cluster. This topic is consumed by the Flink job {@link StratumStatistics}.
 */
public class KafkaStreamProducer {

    private final String filePath;
    private final String topicName;
    private final Integer partitions;
    private final Producer<Long, String> producer;

    /**
     * @param filePath   The absolute path of the data set file
     * @param topicName  The topic name that the producer will write the data to
     * @param partitions The number of partitions of the topic that is provided
     */
    public KafkaStreamProducer(String filePath, String topicName, Integer partitions) {
        this.filePath = filePath;
        this.topicName = topicName;
        this.partitions = partitions;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConstants.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        props.put(ProducerConfig.ACKS_CONFIG, KafkaConstants.ACKS);
//        props.put(ProducerConfig.RETRIES_CONFIG, KafkaConstants.RETRIES);
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, KafkaConstants.BATCH_SIZE);
//        props.put(ProducerConfig.LINGER_MS_CONFIG, KafkaConstants.LINGER_MS);
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, KafkaConstants.BUFFER_MEMORY);

        this.producer = new KafkaProducer<>(props);
        System.out.println("INFO [KafkaStreamProducer]:  " + this.toString());

    }

    public void runProducer() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            System.out.println("INFO [KafkaStreamProducer]:  runProducer(), read file");
            String line;
            long cnt = 0;
            while ((line = br.readLine()) != null) {

                if (cnt > 0) {
                    producer.send(new ProducerRecord<>(topicName, ((int) cnt) % this.partitions, cnt, line));
                }
                cnt += 1;

//                if (cnt % 100 == 0) {
//                    System.out.println("INFO [KafkaStreamProducer]:  runProducer(), cnt=" + cnt + " ,line:" + line);
//                }
            }
            br.close();

        } catch (IOException e) {
            System.out.println("Something went wrong while producing the data.");
            e.printStackTrace();
        } finally {
            System.out.println("Terminating the Kafka producer.");
            producer.close();
        }
    }

    @Override
    public String toString() {
        return "filePath: " + filePath +
                ", topicName: " + topicName +
                ", partitions: " + partitions;
    }

    /**
     * @param args You have to provide 3 arguments otherwise DEFAULT values will be used.<br>
     *             <ol>
     *             <li> args[0]={@link #filePath} Dataset file path, DEFAULT VALUE:"/src/main/java/dataset/2018-04-04.csv"
     *             <li> args[1]={@link #topicName} Name of Kafka Topic, DEFAULT VALUE:"OpenAQ"
     *             <li> args[2]={@link #partitions}, Number of Kafka Partitions, DEFAULT VALUE:"4"
     *             </ol>
     */
    public static void main(String[] args) {

        KafkaStreamProducer kafkaProducer;

        if (args.length == 3) {
            kafkaProducer = new KafkaStreamProducer(args[0], args[1], Integer.parseInt(args[2]));
        } else {

            String data_set_path;
            if (SystemUtils.IS_OS_LINUX)
                data_set_path = new File("").getAbsolutePath() + "/src/main/java/dataset/2018-04-04.csv";
            else if(SystemUtils.IS_OS_WINDOWS)
                data_set_path = new File("").getAbsolutePath() + "\\src\\main\\java\\dataset\\2018-04-04.csv";
            else
                throw new RuntimeException("Incompatible operating system. Run the project on a LINUX or a Windows OS.");

            kafkaProducer = new KafkaStreamProducer(
                    data_set_path,
                    "OpenAQ",
                    4);
        }

        kafkaProducer.runProducer();
    }
}
