package pt.is.assignment3.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor // Lombok annotations to create getters, setters and constructor (boilerplate code)
public class Trip {
    private long id;
    private long routeId;
    private long passengerId;
    private String passengerName;
    private String transportType;
    
    // The next fields are specified in project assignment, but are redundant because they are already in Route entity, may be only necessary for debugging
    // private String origin, destination;
    // private String transportType;
}
