package pt.is.assignment3.entity;

import org.springframework.data.annotation.Id;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data @NoArgsConstructor // Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq10 { // average number of passengers per transport type

    @Id
    @Setter(AccessLevel.NONE) // prevent the ID from being changed, since its comes from the database
    private String key;
    private String transportType;
    private double averagePassengers;

    public ResultsReq10(String transportType, double averagePassengers) {
        this.transportType = transportType;
        this.averagePassengers = averagePassengers;
    }

    @Override
    public String toString() { // postgres schema
        return String.format("""
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "string", "optional": false, "field": "transport_type"},
                    {"type": "double", "optional": false, "field": "average_passengers"}
                ],
                "optional": false
            },
            "payload": {
                "transport_type": "%s",
                "average_passengers": %.2f
            }
        }
        """, transportType, averagePassengers);
    }
        
}
