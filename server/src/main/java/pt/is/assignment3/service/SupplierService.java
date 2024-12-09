package pt.is.assignment3.service;

import org.springframework.stereotype.Service;

import org.springframework.beans.factory.annotation.Autowired;

import pt.is.assignment3.repository.SupplierRepository;

import pt.is.assignment3.entity.RouteSupplier;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class SupplierService {
    @Autowired
    private SupplierRepository supplierRepository;

    public Flux<RouteSupplier> getAllSuppliers() {
        return supplierRepository.findAll();
    }
    
    public Mono<RouteSupplier> addSupplier(RouteSupplier supplier) {
        return supplierRepository.save(supplier);
    }
}
