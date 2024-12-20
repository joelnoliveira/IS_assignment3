package pt.is.assignment3.service;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;

import pt.is.assignment3.entity.*;

import pt.is.assignment3.repository.*;

import reactor.core.publisher.Flux;

@Service
public class ResultsService {
    
    @Autowired
    private Req04Repository req04Repository;
    @Autowired
    private Req05Repository req05Repository;
    @Autowired
    private Req06Repository req06Repository;
    @Autowired
    private Req07Repository req07Repository;
    @Autowired
    private Req08Repository req08Repository;
    @Autowired
    private Req09Repository req09Repository;
    @Autowired
    private Req10Repository req10Repository;
    @Autowired
    private Req11Repository req11Repository;
    @Autowired
    private Req12Repository req12Repository;
    @Autowired
    private Req13Repository req13Repository;
    @Autowired
    private Req14Repository req14Repository;
    @Autowired
    private Req15Repository req15Repository;
    @Autowired
    private Req16Repository req16Repository;

    

    public Flux<ResultsReq04> getReq4() {
        return req04Repository.findAll();
    }

    public Flux<ResultsReq05> getReq5() {
        return req05Repository.findAll();
    }

    public Flux<ResultsReq06> getReq6() {
        return req06Repository.findAll();
    }

    public Flux<ResultsReq07> getReq7() {
        return req07Repository.findAll();
    }

    public Flux<ResultsReq08> getReq8() {
        return req08Repository.findAll();
    }

    public Flux<ResultsReq09> getReq9() {
        return req09Repository.findAll();
    }

    public Flux<ResultsReq10> getReq10() {
        return req10Repository.findAll();
    }

    public Flux<ResultsReq11> getReq11() {
        return req11Repository.findAll();
    }

    public Flux<ResultsReq12> getReq12() {
        return req12Repository.findAll();
    }

    public Flux<ResultsReq13> getReq13() {
        return req13Repository.findAll();
    }

    public Flux<ResultsReq14> getReq14() {
        return req14Repository.findAll();
    }

    public Flux<ResultsReq15> getReq15() {
        return req15Repository.findAll();
    }

    public Flux<ResultsReq16> getReq16() {
        return req16Repository.findAll();
    }

}
