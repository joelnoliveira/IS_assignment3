package pt.is.assignment3.kafka.utils;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

public class KafkaJSONSerde<T> implements Serde<T> {

    private KafkaJSONSerializer<T> serializer;
    private KafkaJSONDeserializer<T> deserializer;

    public KafkaJSONSerde(Class<T> targetType) {
        serializer = new KafkaJSONSerializer<>();
        deserializer = new KafkaJSONDeserializer<>(targetType);
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}