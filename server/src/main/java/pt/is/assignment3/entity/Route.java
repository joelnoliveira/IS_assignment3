package pt.is.assignment3.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor // Lombok annotations to create getters, setters and constructor (boilerplate code)
public class Route {
    private int capacity;
    private long id;
    private long operatorId;
    private String origin, destination;
    private String transportType;
    private String operatorName;

    public String myToString() { // postgres schema
        return String.format("""
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int32", "optional": false, "field": "capacity"},
                    {"type": "int64", "optional": false, "field": "id"},
                    {"type": "int64", "optional": false, "field": "operator_id"},
                    {"type": "string", "optional": false, "field": "origin"},
                    {"type": "string", "optional": false, "field": "destination"},
                    {"type": "string", "optional": false, "field": "transport_type"},
                    {"type": "string", "optional": false, "field": "operator_name"}
                ],
                "optional": false
            },
            "payload": {
                "capacity": %d,
                "id": %d,
                "operator_id": %d,
                "origin": "%s",
                "destination": "%s",
                "transport_type": "%s",
                "operator_name": "%s"
            }
        }
        """, capacity, id, operatorId, origin, destination, transportType, operatorName);
    }
}
