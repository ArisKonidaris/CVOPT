package dataStructures;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * A custom Flink deserialization schema. The Flink Kafka Consumer
 * uses this Deserialization schema to create {@link KafkaRecord} records.
 */
public class MySchema implements KeyedDeserializationSchema<KafkaRecord> {
    @Override
    public KafkaRecord deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) {
        LongDeserializer keyDeserializeer = new LongDeserializer();
        StringDeserializer valueDeserializer = new StringDeserializer();
        KafkaRecord rec = new KafkaRecord();

        rec.key = keyDeserializeer.deserialize(topic, messageKey);
        rec.value = valueDeserializer.deserialize(topic, message);
        rec.topic = topic;
        rec.partition = partition;
        rec.offset = offset;

        return rec;
    }

    @Override
    public boolean isEndOfStream(KafkaRecord kafkaRecord) {
        return false;
    }

    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
        return null;
    }
}