package pt.is.assignment3.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import org.springframework.stereotype.Repository;

import pt.is.assignment3.entity.ResultsReq07;

@Repository
public interface Req07Repository extends ReactiveCrudRepository<ResultsReq07, String> {
    
}
