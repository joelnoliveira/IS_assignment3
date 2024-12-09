package pt.is.assignment3.controller;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import org.springframework.beans.factory.annotation.Autowired;

import pt.is.assignment3.entity.RouteCapacity;
import pt.is.assignment3.service.CapacityService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/route-capacity")
public class CapacityController {
    
    private final CapacityService capacityService;

    @Autowired
    public CapacityController(CapacityService capacityService) {
        this.capacityService = capacityService;
    }

    @GetMapping
    public Flux<RouteCapacity> getAllCapacities() {
        return capacityService.getAllCapacities();
    }

    @PostMapping
    public Mono<RouteCapacity> addCapacity(@RequestBody RouteCapacity capacity) {
        return capacityService.addCapacity(capacity);
    }
}
