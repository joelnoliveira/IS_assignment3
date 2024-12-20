package pt.is.assignment3.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import org.springframework.stereotype.Repository;

import pt.is.assignment3.entity.ResultsReq13;

@Repository
public interface Req13Repository extends ReactiveCrudRepository<ResultsReq13, String> {
    
}
