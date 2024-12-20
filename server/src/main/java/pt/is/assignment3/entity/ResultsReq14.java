package pt.is.assignment3.entity;

import org.springframework.data.annotation.Id;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data @NoArgsConstructor // Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq14 { // the least occupied transport type in the last hour

    @Id
    @Setter(AccessLevel.NONE) // prevent the ID from being changed, since its comes from the database
    private String key;
    private String transportType;

    public ResultsReq14(String transportType) {
        this.transportType = transportType;
    }

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
