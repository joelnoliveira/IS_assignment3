package pt.is.assignment3.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor @AllArgsConstructor// Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq13 { // the most used transport type in the last hour

    private String transportType;

    @Override
    public String toString() { // postgres schema
        return String.format("""
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "string", "optional": false, "field": "transport_type"}
                ],
                "optional": false
            },
            "payload": {
                "transport_type": "%s"
            }
        }
        """, transportType);
    }
}
