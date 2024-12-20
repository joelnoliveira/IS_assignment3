package pt.is.assignment3.entity;

import org.springframework.data.annotation.Id;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data @NoArgsConstructor // Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq16 { // the name of the passenger with the most trips

    @Id
    @Setter(AccessLevel.NONE) // prevent the ID from being changed, since its comes from the database
    private String key;
    private String passengerName;

    public ResultsReq16(String passengerName) {
        this.passengerName = passengerName;
    }

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