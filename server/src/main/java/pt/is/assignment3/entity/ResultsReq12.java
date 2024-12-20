package pt.is.assignment3.entity;

import org.springframework.data.annotation.Id;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data @NoArgsConstructor // Lombok annotations to create getters, setters and constructor (boilerplate code)
public class ResultsReq12 { // routes with the least occupancy per transport type

    @Id
    @Setter(AccessLevel.NONE) // prevent the ID from being changed, since its comes from the database
    private String key;
    private long routeId;
    private String transportType;

    public ResultsReq12(long routeId, String transportType) {
        this.routeId = routeId;
        this.transportType = transportType;
    }

    @Override
    public String toString() { // postgres schema
        return String.format("""
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int64", "optional": false, "field": "route_id"},
                    {"type": "string", "optional": false, "field": "transport_type"}
                ],
                "optional": false
            },
            "payload": {
                "route_id": %d,
                "transport_type": "%s"
            }
        }
        """, routeId, transportType);
    }   
}
