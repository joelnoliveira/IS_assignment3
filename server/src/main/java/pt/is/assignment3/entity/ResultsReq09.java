package pt.is.assignment3.entity;

import org.springframework.data.annotation.Id;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data @NoArgsConstructor // Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq09 {  // total occupancy percentage

    @Id
    @Setter(AccessLevel.NONE) // prevent the ID from being changed, since its comes from the database
    private String key;
    private double totalOccupancyPercentage;

    public ResultsReq09(double totalOccupancyPercentage) {
        this.totalOccupancyPercentage = totalOccupancyPercentage;
    }

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
