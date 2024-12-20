package pt.is.assignment3.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import org.springframework.stereotype.Repository;

import pt.is.assignment3.entity.ResultsReq14;

@Repository
public interface Req14Repository extends ReactiveCrudRepository<ResultsReq14, String> {
    
}
