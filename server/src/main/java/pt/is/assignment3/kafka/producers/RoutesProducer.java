package pt.is.assignment3.kafka.producers;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

import pt.is.assignment3.entity.Route;

public class RoutesProducer {

    private final static Long [] operatorId = {1L,2L,3L};
    private final static String[] operators = {"OperatorA", "OperatorB", "OperatorC"};
    
    private final static String[] transportTypes = {"Bus", "Taxi", "Train", "Metro", "Scooter"};
    private final static int [] capacities = {60, 4, 300, 300, 1};
    
    private final static String[] place = {"CityA", "CityB", "CityC", "CityD", "CityE", "CityF", "CityG", "CityH", "CityI", "CityJ"};
    
    private static final int MAX_COMBINATIONS = operatorId.length * transportTypes.length * place.length * place.length;
    
    private static Set<String> generatedRoutes = new HashSet<>();


    private static Route routeGenerator(Long routeId) {

        int randomOperator;
        int randomTransport;
        int randomOrigin;
        int randomDestination;
        String routeKey;

        do{
            randomOperator = (int) (Math.random() * operatorId.length);
            randomTransport = (int) (Math.random() * transportTypes.length);
            randomOrigin = (int) (Math.random() * place.length);
            randomDestination = (int) (Math.random() * place.length);

            routeKey = String.format("%s-%s-%s-%s", operatorId[randomOperator], transportTypes[randomTransport], place[randomOrigin], place[randomDestination]); // key to avoid duplicate routes

        }while(generatedRoutes.add(routeKey) == false);

        Route route = new Route();
        route.setId(routeId);
        route.setOperatorId(operatorId[randomOperator]);
        route.setOperatorName(operators[randomOperator]);

        route.setTransportType(transportTypes[randomTransport]);
        route.setCapacity(capacities[randomTransport]);

        route.setOrigin(place[randomOrigin]);
        route.setDestination(place[randomDestination]);

        return route;
    }

    // TODO do constant version for teacher scnerio testing
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

        while (routeId < MAX_COMBINATIONS) { // while there are still possible combinations

            System.out.println("Press Enter to produce a new route");
            System.in.read();

            Route route = routeGenerator(routeId++); // generate a new route
            
            ProducerRecord<String, Route> record = new ProducerRecord<>(outputTopic, route.getId()+"",route); // defined OperatorId as key because it seemed the most appropriate
            producer.send(record);

            // give feedback
            System.out.printf("Sent to topic %s: %s", outputTopic, route);
        }

        producer.close();
        System.out.println("Combination limit reached. Exiting...");
    }
}
