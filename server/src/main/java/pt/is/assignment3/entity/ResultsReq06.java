package pt.is.assignment3.entity;

import org.springframework.data.annotation.Id;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data @NoArgsConstructor // Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq06 { // Occupancy percentage per route

    @Id
    @Setter(AccessLevel.NONE) // prevent the ID from being changed, since its comes from the database
    private String key;
    private long routeId;
    private double occupancyPercentage;

    public ResultsReq06(long routeId, double occupancyPercentage) {
        this.routeId = routeId;
        this.occupancyPercentage = occupancyPercentage;
    }

    @Override
    public String toString() { // postgres schema
        return String.format("""
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int64", "optional": false, "field": "route_id"},
                    {"type": "double", "optional": false, "field": "occupancy_percentage"}
                ],
                "optional": false
            },
            "payload": {
                "route_id": %d,
                "occupancy_percentage": %.2f
            }
        }
        """, routeId, occupancyPercentage);
    }   
}
