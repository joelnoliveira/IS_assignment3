package pt.is.assignment3.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor // Lombok annotations to create getters, setters and constructor (boilerplate code)
public class Trip {
    private long id;
    private long routeId;
    private long passengerId;
    private String passengerName;
    private String transportType;
    
    // The next fields are specified in project assignment, but are redundant because they are already in Route entity, may be only necessary for debugging
    // private String origin, destination;

    public String myToString() { // postgres schema
        return String.format("""
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int64", "optional": false, "field": "id"},
                    {"type": "int64", "optional": false, "field": "route_id"},
                    {"type": "int64", "optional": false, "field": "passenger_id"},
                    {"type": "string", "optional": false, "field": "passenger_name"},
                    {"type": "string", "optional": false, "field": "transport_type"}
                ],
                "optional": false
            },
            "payload": {
                "id": %d,
                "route_id": %d,
                "passenger_id": %d,
                "passenger_name": "%s",
                "transport_type": "%s"
            }
        }
        """, id, routeId, passengerId, passengerName, transportType);
    }

}
