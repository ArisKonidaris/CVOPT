package dataStructures;

import java.io.Serializable;

/**
 * A class representing the records consumed by a Flink Kafka Consumer.
 * The Kafka byte message will be deserialized into this Serializable POJO class
 */
public class KafkaRecord implements Serializable {
    public Long key;
    public String value;
    public long offset;
    public int partition;
    public String topic;

    public KafkaRecord() {
    }

    public Long getKey() {
        return key;
    }

    public void setKey(Long key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}