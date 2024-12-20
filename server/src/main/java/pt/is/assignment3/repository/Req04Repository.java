package pt.is.assignment3.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import org.springframework.stereotype.Repository;

import pt.is.assignment3.entity.ResultsReq04;

@Repository
public interface Req04Repository extends ReactiveCrudRepository<ResultsReq04, String> {
    
}
