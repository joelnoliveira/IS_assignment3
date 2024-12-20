package pt.is.assignment3.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import org.springframework.stereotype.Repository;

import pt.is.assignment3.entity.ResultsReq15;

@Repository
public interface Req15Repository extends ReactiveCrudRepository<ResultsReq15, String> {
    
}
