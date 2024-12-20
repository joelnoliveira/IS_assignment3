package pt.is.assignment3.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import org.springframework.stereotype.Repository;

import pt.is.assignment3.entity.ResultsReq16;

@Repository
public interface Req16Repository extends ReactiveCrudRepository<ResultsReq16, String> {
    
}
