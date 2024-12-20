package pt.is.assignment3.entity;

import org.springframework.data.annotation.Id;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data @NoArgsConstructor// Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq08 { // Seats count (all summed for all routes)

    @Id
    @Setter(AccessLevel.NONE) // prevent the ID from being changed, since its comes from the database
    private String key;
    private long totalSeats;

    public ResultsReq08(long totalSeats) {
        this.totalSeats = totalSeats;
    }

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
