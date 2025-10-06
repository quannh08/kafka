package com.kafka.kafka.service;

import com.google.gson.Gson;
import com.kafka.kafka.dto.request.TransactionRequest;
import com.kafka.kafka.entity.Transaction;
import com.kafka.kafka.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j(topic = "CONSUMER-SERVICE")
public class ConsumerService {

    private final TransactionRepository transactionRepository;

    private final BatchMessageService batchMessageService;

    @KafkaListener(topics = "transaction_logs", groupId = "demo-group")
    public void listenGroup(String message) {
        log.info("Received raw: {}", message);
        batchMessageService.receiveMessage(message);
    }


}
