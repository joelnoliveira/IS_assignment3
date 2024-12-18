package pt.is.assignment3.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor @AllArgsConstructor// Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq12 { // routes with the least occupancy per transport type

    private long routeId;
    private String transportType;

    @Override
    public String toString() { // postgres schema
        return String.format("""
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int64", "optional": false, "field": "route_id"},
                    {"type": "string", "optional": false, "field": "transport_type"}
                ],
                "optional": false
            },
            "payload": {
                "route_id": %d,
                "transport_type": "%s"
            }
        }
        """, routeId, transportType);
    }   
}
