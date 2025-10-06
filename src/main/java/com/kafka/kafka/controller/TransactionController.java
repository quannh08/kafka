package com.kafka.kafka.controller;

import com.kafka.kafka.dto.request.TransactionRequest;
import com.kafka.kafka.service.ProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/transaction")
@Slf4j(topic = "TRANSACTION-CONTROLLER")
@RequiredArgsConstructor
public class TransactionController {
    private final ProducerService producerService;

    @PostMapping("/")
    public void sendData(@RequestBody TransactionRequest request){
        log.info("Sending data to Kafka");
        producerService.sendTransaction(request);
    }

    @PostMapping("/send")
    public void sendRequest() throws InterruptedException {
        log.info("Sending request to Kafka");
        producerService.sendRandomTransaction();
    }


}
