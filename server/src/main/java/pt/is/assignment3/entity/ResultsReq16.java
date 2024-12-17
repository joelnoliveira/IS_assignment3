package pt.is.assignment3.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor @AllArgsConstructor// Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq16 { // the name of the passenger with the most trips

    private String passengerName;

    @Override
    public String toString() { // postgres schema
        return String.format("""
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "string", "optional": false, "field": "passenger_name"}
                ],
                "optional": false
            },
            "payload": {
                "passenger_name": "%s"
            }
        }
        """, passengerName);
    }
        
}