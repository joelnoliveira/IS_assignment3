package pt.is.assignment3.entity;

import org.springframework.data.annotation.Id;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data @NoArgsConstructor // Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq05 { // Seats per route
    
    @Id
    @Setter(AccessLevel.NONE) // prevent the ID from being changed, since its comes from the database
    private String key;
    private long routeId;
    private long seatCount;

    public ResultsReq05(long routeId, long seatCount) {
        this.routeId = routeId;
        this.seatCount = seatCount;
    }

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
