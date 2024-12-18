package pt.is.assignment3.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor @AllArgsConstructor// Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq08 { // Seats count (all summed for all routes)

    private long totalSeats;

    @Override
    public String toString() { // postgres schema
        return String.format("""
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int64", "optional": false, "field": "total_seats"}
                ],
                "optional": false
            },
            "payload": {
                "total_seats": %d
            }
        }
        """, totalSeats);
    }        
}
