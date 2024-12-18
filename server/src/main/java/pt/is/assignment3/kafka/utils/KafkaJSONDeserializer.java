package pt.is.assignment3.kafka.utils;

import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaJSONDeserializer<T> implements Deserializer<T> {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> targetType;

    public KafkaJSONDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            System.out.println("Null data received when deserializing");
            return null;
        }
        try {
            // convert to String and print
            String dataStr = new String(data);
            // System.out.println("Deserializing to an object: " + dataStr);
            return objectMapper.readValue(data, targetType);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error deserializing JSON message");
            return null;
        }
    }
}