package pt.is.assignment3.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import org.springframework.stereotype.Repository;

import pt.is.assignment3.entity.ResultsReq12;

@Repository
public interface Req12Repository extends ReactiveCrudRepository<ResultsReq12, String> {
    
}
