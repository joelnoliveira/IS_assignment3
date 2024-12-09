package pt.is.assignment3.repository;

import org.springframework.stereotype.Repository;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import pt.is.assignment3.entity.RouteSupplier;

@Repository
public interface SupplierRepository extends ReactiveCrudRepository<RouteSupplier, Long> {
    
}
