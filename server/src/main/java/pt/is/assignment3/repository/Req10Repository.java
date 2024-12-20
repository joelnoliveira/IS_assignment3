package pt.is.assignment3.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import org.springframework.stereotype.Repository;

import pt.is.assignment3.entity.ResultsReq10;

@Repository
public interface Req10Repository extends ReactiveCrudRepository<ResultsReq10, String> {
    
}
