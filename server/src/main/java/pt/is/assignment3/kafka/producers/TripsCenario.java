package pt.is.assignment3.kafka.producers;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;

import pt.is.assignment3.entity.Trip;



public class TripsCenario {
 
    private static List<Trip> initializer(){

        Trip trip;
        ArrayList<Trip> trips = new ArrayList<>();

        trip = new Trip();
        trip.setId(1L);
        trip.setRouteId(4L);
        trip.setPassengerId(1L);
        trip.setPassengerName("Passenger 1");
        trip.setTransportType("Bus");

        //add trip to an arraylist
        trips.add(trip);

        trip = new Trip();
        trip.setId(2L);
        trip.setRouteId(2L);
        trip.setPassengerId(2L);
        trip.setPassengerName("Passenger 2");
        trip.setTransportType("Taxi");

        trips.add(trip);

        trip = new Trip();
        trip.setId(3L);
        trip.setRouteId(1L);
        trip.setPassengerId(3L);
        trip.setPassengerName("Passenger 3");
        trip.setTransportType("Bus");

        trips.add(trip);

        trip = new Trip();
        trip.setId(4L);
        trip.setRouteId(1L);
        trip.setPassengerId(4L);
        trip.setPassengerName("Passenger 4");
        trip.setTransportType("Bus");

        trips.add(trip);

        trip = new Trip();
        trip.setId(5L);
        trip.setRouteId(3L);
        trip.setPassengerId(1L);
        trip.setPassengerName("Passenger 1");
        trip.setTransportType("Train");

        trips.add(trip);

        // add trip with a route that doesn't exist
        trip = new Trip();
        trip.setId(6L);
        trip.setRouteId(1501L);
        trip.setPassengerId(1L);
        trip.setPassengerName("Passenger 1");
        trip.setTransportType("Teleportation");

        trips.add(trip);

        trip = new Trip();
        trip.setId(7L);
        trip.setRouteId(3L);
        trip.setPassengerId(7L);
        trip.setPassengerName("Passenger 7");
        trip.setTransportType("Train");

        trips.add(trip);

        trip = new Trip();
        trip.setId(8L);
        trip.setRouteId(3L);
        trip.setPassengerId(7L);
        trip.setPassengerName("Passenger 7");
        trip.setTransportType("Train");

        trips.add(trip);

        trip = new Trip();
        trip.setId(9L);
        trip.setRouteId(3L);
        trip.setPassengerId(7L);
        trip.setPassengerName("Passenger 7");
        trip.setTransportType("Train");

        trips.add(trip);

        return trips;
    }

    public static void main(String[] args) throws Exception {
        
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

        ArrayList<Trip> trips = (ArrayList<Trip>) initializer();

        for (Trip trip : trips) {

            System.out.println("Press Enter to produce a new trip");
            System.in.read();

            ProducerRecord<String, Trip> record = new ProducerRecord<>(outputTopic, trip.getRouteId()+"", trip);// define the key as the routeId, because it seamed the more appropriate
            producer.send(record);

            //give feedback
            System.out.printf("Sent to topic %s: %s\n", outputTopic, trip);
        }
        producer.close();
    }
}
