package pt.is.assignment3.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import org.springframework.stereotype.Repository;

import pt.is.assignment3.entity.ResultsReq05;

@Repository
public interface Req05Repository extends ReactiveCrudRepository<ResultsReq05, String> {
    
}
