package pt.is.assignment3.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import org.springframework.stereotype.Repository;

import pt.is.assignment3.entity.ResultsReq06;

@Repository
public interface Req06Repository extends ReactiveCrudRepository<ResultsReq06, Long> {
    
}
