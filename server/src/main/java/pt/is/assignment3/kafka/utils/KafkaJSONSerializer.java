package pt.is.assignment3.kafka.utils;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaJSONSerializer<T> implements Serializer<T> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            if (data == null) {
                System.out.println("Received null data in serialization");
                return null;
            }
            return objectMapper.writeValueAsBytes(data); // Serialize object to JSON bytes
        } catch (Exception e) {
            e.printStackTrace();

            System.out.println("Error serializing JSON message");
            return null;
        }
    }
    
}
