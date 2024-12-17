package pt.is.assignment3.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor @AllArgsConstructor// Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq05 { // Seats per route

    private Long routeId;
    private Long seatCount;

    @Override
    public String toString() { // postgres schema
        return String.format("""
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int64", "optional": false, "field": "route_id"},
                    {"type": "int64", "optional": false, "field": "seat_count"}
                ],
                "optional": false
            },
            "payload": {
                "route_id": %d,
                "seat_count": %d
            }
        }
        """, routeId, seatCount);
    }
        
}
