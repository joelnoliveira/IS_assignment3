package pt.is.assignment3.kafka.utils;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaJSONSerializer implements Serializer<Object> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Object data) {
        try {
            return objectMapper.writeValueAsBytes(data); // Serialize object to JSON bytes
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
}
