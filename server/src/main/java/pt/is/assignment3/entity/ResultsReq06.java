package pt.is.assignment3.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor @AllArgsConstructor// Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq06 { // Occupancy percentage per route

    private Long routeId;
    private double occupancy;

    @Override
    public String toString() { // postgres schema
        return String.format("""
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int64", "optional": false, "field": "route_id"},
                    {"type": "double", "optional": false, "field": "occupancy"}
                ],
                "optional": false
            },
            "payload": {
                "route_id": %d,
                "occupancy": %.2f
            }
        }
        """, routeId, occupancy);
    }   
}
