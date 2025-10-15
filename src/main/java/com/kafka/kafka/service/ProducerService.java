package com.kafka.kafka.service;


import com.google.gson.Gson;
import com.kafka.kafka.dto.request.TransactionRequest;
import com.kafka.kafka.entity.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

@Service
@RequiredArgsConstructor
@Slf4j(topic = "TRANSACTION-SERVICE")
public class ProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;


    private final Gson gson;


    public void sendTransaction(TransactionRequest request) {
        log.info("send Transaction");

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("notification", gson.toJson(request));
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                // gửi thành công
                System.out.println("Sent transaction=[" +gson.toJson(request) +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");

            } else {
                // gửi thất bại
                System.out.println("Unable to send message=[" +
                        gson.toJson(request) + "] due to : " + ex.getMessage());
            }
        });
    }

    public void sendRandomTransaction() throws InterruptedException {
        log.info("send 1000 Transaction");

        int targetPerMinute = 1000;
        int perSecond = (int) Math.ceil(targetPerMinute / 60.0);
//        int cnt=0;
        for (int i = 0; i < 60; i++) {
            for (int j = 0; j < perSecond; j++) {
                TransactionRequest tx = generateRandomTransaction();
                kafkaTemplate.send("transaction_log",tx.getUserId(), gson.toJson(tx));
//                log.info("Send transaction number: {}",cnt++);
            }
            Thread.sleep(1000);
        }
    }

    private TransactionRequest generateRandomTransaction(){
        return TransactionRequest.builder()
                .id(ThreadLocalRandom.current().nextLong(1,500000))
                .userId(UUID.randomUUID().toString())
                .amount(ThreadLocalRandom.current().nextLong(1000,20000000))
                .build();
    }
}
