package pt.is.assignment3.service;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;

import pt.is.assignment3.entity.ResultsReq04;

import pt.is.assignment3.repository.*;

import reactor.core.publisher.Flux;

@Service
public class ResultsService {
    
    @Autowired
    private Req04Repository req04Repository, req05Repository, req06Repository, req07Repository, req08Repository, req09Repository, req10Repository, req11Repository, req12Repository, req13Repository, req14Repository, req15Repository, req16Repository;
    

    public Flux<ResultsReq04> getReq4() {
        return req04Repository.findAll();
    }

    public Flux<ResultsReq04> getReq5() {
        return req05Repository.findAll();
    }

    public Flux<ResultsReq04> getReq6() {
        return req06Repository.findAll();
    }

    public Flux<ResultsReq04> getReq7() {
        return req07Repository.findAll();
    }

    public Flux<ResultsReq04> getReq8() {
        return req08Repository.findAll();
    }

    public Flux<ResultsReq04> getReq9() {
        return req09Repository.findAll();
    }

    public Flux<ResultsReq04> getReq10() {
        return req10Repository.findAll();
    }

    public Flux<ResultsReq04> getReq11() {
        return req11Repository.findAll();
    }

    public Flux<ResultsReq04> getReq12() {
        return req12Repository.findAll();
    }

    public Flux<ResultsReq04> getReq13() {
        return req13Repository.findAll();
    }

    public Flux<ResultsReq04> getReq14() {
        return req14Repository.findAll();
    }

    public Flux<ResultsReq04> getReq15() {
        return req15Repository.findAll();
    }

    public Flux<ResultsReq04> getReq16() {
        return req16Repository.findAll();
    }

}
