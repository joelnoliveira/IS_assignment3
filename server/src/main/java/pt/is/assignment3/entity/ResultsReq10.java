package pt.is.assignment3.entity;

import org.springframework.data.annotation.Id;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor @AllArgsConstructor// Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq10 { // average number of passengers per transport type

    private String transportType;
    private double averagePassengers;

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
                "transport_type": %s,
                "average_passengers": %.2f
            }
        }
        """, transportType, averagePassengers);
    }
        
}
