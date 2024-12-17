package pt.is.assignment3.entity;

import org.springframework.data.annotation.Id;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor @AllArgsConstructor// Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq04 { // Get the passengers per route

    // @Id
    // private Long id;

    private Long routeId;
    private Long passengerCount;

    @Override
    public String toString() { // postgres schema
        return String.format("""
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int64", "optional": false, "field": "route_id"},
                    {"type": "int64", "optional": false, "field": "passenger_count"}
                ],
                "optional": false
            },
            "payload": {
                "route_id": %d,
                "passenger_count": %d
            }
        }
        """, routeId, passengerCount);
    }
        
}

// """
//         {
//         "schema":{
//         "type":"struct",
//         "fields":[
//         {"type":"BINT","optional":false,"field":"route_id"},
//         {"type":"BINT","optional":false,"field":"passenger_count"}
//         ],
//         "optional":false,
//         "name":"req4"
//         },
//         "payload":{
//         "route_id":210,
//         "transport_type":"Train"}
//         }
//         """;

// return """
//         {
//         "schema":{
//         "type":"struct",
//         "fields":[
//         {"type":"int64","optional":false,"field":"id"},
//         {"type":"int32","optional":false,"field":"capacity"},
//         {"type":"string","optional":false,"field":"transport_type"}
//         ],
//         "optional":false,
//         "name":"route_capacity"
//         },
//         "payload":{
//         "id":1,
//         "capacity":210,
//         "transport_type":"Train"}
//         }
//         """;