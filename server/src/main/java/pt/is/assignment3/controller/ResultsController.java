package pt.is.assignment3.controller;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import org.springframework.beans.factory.annotation.Autowired;

import pt.is.assignment3.entity.ResultsReq04;
import pt.is.assignment3.service.ResultsService;

import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/results")
public class ResultsController {

    private final ResultsService resultsService;

    @Autowired
    public ResultsController(ResultsService resultsService) {
        this.resultsService = resultsService;
    }

    @GetMapping("/req4")
    public Flux<ResultsReq04> getReq4() {
        return resultsService.getReq4();
    }

    @GetMapping("/req5")
    public Flux<ResultsReq04> getReq5() {
        return resultsService.getReq5();
    }

    @GetMapping("/req6")
    public Flux<ResultsReq04> getReq6() {
        return resultsService.getReq6();
    }

    @GetMapping("/req7")
    public Flux<ResultsReq04> getReq7() {
        return resultsService.getReq7();
    }

    @GetMapping("/req8")
    public Flux<ResultsReq04> getReq8() {
        return resultsService.getReq8();
    }

    @GetMapping("/req9")
    public Flux<ResultsReq04> getReq9() {
        return resultsService.getReq9();
    }

    @GetMapping("/req10")
    public Flux<ResultsReq04> getReq10() {
        return resultsService.getReq10();
    }

    @GetMapping("/req11")
    public Flux<ResultsReq04> getReq11() {
        return resultsService.getReq11();
    }

    @GetMapping("/req12")
    public Flux<ResultsReq04> getReq12() {
        return resultsService.getReq12();
    }

    @GetMapping("/req13")
    public Flux<ResultsReq04> getReq13() {
        return resultsService.getReq13();
    }

    @GetMapping("/req14")
    public Flux<ResultsReq04> getReq14() {
        return resultsService.getReq14();
    }

    @GetMapping("/req15")
    public Flux<ResultsReq04> getReq15() {
        return resultsService.getReq15();
    }

    @GetMapping("/req16")
    public Flux<ResultsReq04> getReq16() {
        return resultsService.getReq16();
    }

}