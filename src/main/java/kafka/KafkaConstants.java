package kafka;

/**
 * Kafka static final parameters used to set up Kafka producer properties
 */
public final class KafkaConstants {
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String CLIENT_ID = "client1";

    /////////////////////// Other useful parameters (currently not used) for a Kafka producer //////////////////////////

    public static final String ACKS = "all";
    public static final Integer RETRIES = 0;
    public static final Integer BATCH_SIZE = 16384;
    public static final Integer LINGER_MS = 0;
    public static final Integer BUFFER_MEMORY = 33554432;
    public static final String OFFSET_RESET_LATEST = "latest";
    public static final String OFFSET_RESET_EARLIER = "earliest";
    public static final Integer MAX_POLL_RECORDS = 1;

}
