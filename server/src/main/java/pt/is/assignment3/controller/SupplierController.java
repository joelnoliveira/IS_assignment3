package pt.is.assignment3.controller;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import org.springframework.beans.factory.annotation.Autowired;

import pt.is.assignment3.entity.RouteSupplier;
import pt.is.assignment3.service.SupplierService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/route-supplier")
public class SupplierController {
        
        private final SupplierService supplierService;
    
        @Autowired
        public SupplierController(SupplierService supplierService) {
            this.supplierService = supplierService;
        }
    
        @GetMapping
        public Flux<RouteSupplier> getAllSuppliers() {
            return supplierService.getAllSuppliers();
        }
    
        @PostMapping
        public Mono<RouteSupplier> addSupplier(@RequestBody RouteSupplier supplier) {
            return supplierService.addSupplier(supplier);
        }
}
