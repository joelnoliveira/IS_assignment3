package pt.is.assignment3.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor // Lombok annotations to create getters, setters and constructor (boilerplate code)
public class Route {
    private int capacity;
    private long id;
    private long operatorId;
    private String origin, destination;
    private String transportType;
    private String operatorName;
}
