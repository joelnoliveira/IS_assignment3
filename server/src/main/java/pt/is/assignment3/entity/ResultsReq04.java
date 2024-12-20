package pt.is.assignment3.entity;

import org.springframework.data.annotation.Id;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data @NoArgsConstructor// Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq04 { // Get the passengers per route

    @Id
    @Setter(AccessLevel.NONE) // prevent the ID from being changed, since its comes from the database
    private String key;
    private long routeId;
    private long passengerCount;

    public ResultsReq04(long routeId, long passengerCount) {
        this.routeId = routeId;
        this.passengerCount = passengerCount;
    }

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
