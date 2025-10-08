package com.kafka.kafka.service;

import com.google.gson.Gson;
import com.kafka.kafka.dto.request.TransactionRequest;
import com.kafka.kafka.entity.Transaction;
import com.kafka.kafka.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j(topic = "CONSUMER-SERVICE")
public class ConsumerService {

    private final TransactionRepository transactionRepository;

    private final BatchMessageService batchMessageService;

    @KafkaListener(topics = "transaction_logs", groupId = "demo-group")
    public void listenGroup(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try{
            log.info("Received raw: {}", record.value());
            batchMessageService.receiveMessage(record.value());

            log.info("Sleep 5s!");
            Thread.sleep(5000);
            ack.acknowledge();
        }
        catch (Exception e){
            log.error(e.getMessage());

        }
    }


}
