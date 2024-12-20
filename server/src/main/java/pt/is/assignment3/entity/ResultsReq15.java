package pt.is.assignment3.entity;

import org.springframework.data.annotation.Id;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data @NoArgsConstructor // Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq15 { // the name of the route operator with the most occupancy. Include the value of occupancy

    @Id
    @Setter(AccessLevel.NONE) // prevent the ID from being changed, since its comes from the database
    private String key;
    private String operatorName;
    private double occupancy;

    public ResultsReq15(String operatorName, double occupancy) {
        this.operatorName = operatorName;
        this.occupancy = occupancy;
    }

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
