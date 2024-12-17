package pt.is.assignment3.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor @AllArgsConstructor// Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq07 { // total passengers (all routes summed)

    private Long totalPassengers;

    @Override
    public String toString() { // postgres schema
        return String.format("""
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int64", "optional": false, "field": "total_passengers"}
                ],
                "optional": false
            },
            "payload": {
                "total_passengers": %d
            }
        }
        """, totalPassengers);
    }        
}
