package pt.is.assignment3.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import org.springframework.stereotype.Repository;

import pt.is.assignment3.entity.ResultsReq11;

@Repository
public interface Req11Repository extends ReactiveCrudRepository<ResultsReq11, Long> {
    
}
