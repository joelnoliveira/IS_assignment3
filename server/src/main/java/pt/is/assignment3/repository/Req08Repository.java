package pt.is.assignment3.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import org.springframework.stereotype.Repository;

import pt.is.assignment3.entity.ResultsReq08;

@Repository
public interface Req08Repository extends ReactiveCrudRepository<ResultsReq08, Long> {
    
}
