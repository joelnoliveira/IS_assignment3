package pt.is.assignment3.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor @AllArgsConstructor// Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq15 { // the name of the route operator with the most occupancy. Include the value of occupancy

    private String operatorName;
    private double occupancy;

    @Override
    public String toString() { // postgres schema
        return String.format("""
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "string", "optional": false, "field": "operator_name"},
                    {"type": "double", "optional": false, "field": "occupancy"}
                ],
                "optional": false
            },
            "payload": {
                "operator_name": "%s",
                "occupancy": %.2f
            }
        }
        """, operatorName, occupancy);
    }
}
