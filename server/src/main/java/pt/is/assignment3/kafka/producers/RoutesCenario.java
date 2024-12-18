package pt.is.assignment3.kafka.producers;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import pt.is.assignment3.entity.Route;

public class RoutesCenario {
    
    private static List<Route> initializer(){

        Route route;
        ArrayList<Route> routes = new ArrayList<>();

        route = new Route();
        route.setId(1L);
        route.setOperatorId(1L);
        route.setOperatorName("Operator 1");
        route.setTransportType("Bus");
        route.setCapacity(50);
        route.setOrigin("CityA");
        route.setDestination("CityB");

        //add route to an arraylist
        routes.add(route);

        route = new Route();
        route.setId(2L);
        route.setOperatorId(2L);
        route.setOperatorName("Operator 2");
        route.setTransportType("Taxi");
        route.setCapacity(4);
        route.setOrigin("CityB");
        route.setDestination("CityC");

        routes.add(route);

        route = new Route();
        route.setId(3L);
        route.setOperatorId(1L);
        route.setOperatorName("Operator 1");
        route.setTransportType("Train");
        route.setCapacity(200);
        route.setOrigin("CityC");
        route.setDestination("CityD");

        routes.add(route);

        route = new Route();
        route.setId(4L);
        route.setOperatorId(1L);
        route.setOperatorName("Operator 1");
        route.setTransportType("Bus");
        route.setCapacity(54);
        route.setOrigin("CityD");
        route.setDestination("CityB");

        routes.add(route);

        return routes;
    }

    public static void main(String[] args) throws Exception {
        Long routeId = 0L;
        String outputTopic = "routes";
        
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
        
        Producer<String, Route> producer = new KafkaProducer<>(props);

        ArrayList<Route> routes = (ArrayList<Route>) initializer();
        
        for (Route route : routes) {

            System.out.println("Press Enter to produce a new route");
            System.in.read();

            ProducerRecord<String, Route> record = new ProducerRecord<>(outputTopic, route.getOperatorId()+"",route); // defined OperatorId as key because it seemed the most appropriate
            producer.send(record);

            //give feedback
            System.out.printf("Sent to topic %s: %s", outputTopic, route);
        }
    }
}
