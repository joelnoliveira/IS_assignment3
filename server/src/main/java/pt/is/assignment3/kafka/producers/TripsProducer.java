package pt.is.assignment3.kafka.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

import pt.is.assignment3.entity.Route;
import pt.is.assignment3.entity.Trip;

public class TripsProducer {
    
    private final static int N_ROUTES = 1500;

    private final static Long [] passengerIds = {1L,2L,3L,4L,5L,6L,7L,8L,9L,10L};
    private final static String[] passengerNames = {"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi", "Gerald", "John"};

    private static Trip tripGenerator(Long tripId) {

        int randomPassenger = (int) (Math.random() * passengerIds.length);
        Trip trip = new Trip();
        trip.setId(tripId);
        trip.setPassengerId(passengerIds[randomPassenger]);
        trip.setPassengerName(passengerNames[randomPassenger]);
        trip.setRouteId((long) (Math.random() * N_ROUTES));

        return trip;
    }

    public static void main(String[] args) throws Exception {
        
        Long topicId = 0L;
        String outputTopic = "trips";
        
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "broker1:9092,broker2:9092,broker3:9092");

        //Set acknowledgements for producer requests.      
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0   
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.   
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer", "pt.is.assignment3.kafka.utils.KafkaJSONSerializer");
        
        Producer<String, Trip> producer = new KafkaProducer<>(props);

        while (true) {
            
            System.out.println("Press Enter to produce a new trip");
            System.in.read();

            Trip trip = tripGenerator(topicId++);

            ProducerRecord<String, Trip> record = new ProducerRecord<>(outputTopic, trip.getPassengerId()+"", trip);// define the key as the routeId, because it seamed the more appropriate
            producer.send(record); 

            //give feedback
            System.out.printf("Sent to topic %s: %s\n", outputTopic, trip);
        }

        // TODO: consider adding a shutdown hook to close the producer
    }
}
