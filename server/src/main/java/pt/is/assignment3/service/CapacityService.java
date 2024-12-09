package pt.is.assignment3.service;

import org.springframework.stereotype.Service;

import org.springframework.beans.factory.annotation.Autowired;

import pt.is.assignment3.repository.CapacityRepository;

import pt.is.assignment3.entity.RouteCapacity;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class CapacityService {
    @Autowired
    private CapacityRepository capacityRepository;

    public Flux<RouteCapacity> getAllCapacities() {
        return capacityRepository.findAll();
    }

    public Mono<RouteCapacity> addCapacity(RouteCapacity capacity) {
        return capacityRepository.save(capacity);
    }
}
