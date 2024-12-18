package pt.is.assignment3.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor @AllArgsConstructor// Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq09 {  // total occupancy percentage

    private double totalOccupancyPercentage;

    @Override
    public String toString() {
        return String.format("""
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "double", "optional": false, "field": "total_occupancy_percentage"}
                ],
                "optional": false
            },
            "payload": {
                "total_occupancy_percentage": %.2f
            }
        }
        """, totalOccupancyPercentage);
    }
        
}
