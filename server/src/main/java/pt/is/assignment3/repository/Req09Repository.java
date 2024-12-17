package pt.is.assignment3.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import org.springframework.stereotype.Repository;

import pt.is.assignment3.entity.ResultsReq09;

@Repository
public interface Req09Repository extends ReactiveCrudRepository<ResultsReq09, Long> {
    
}
