package com.kafka.kafka.service;

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

    private final BatchMessageService batchMessageService;


    private final RetryHandlerService retryHandlerService;


    @KafkaListener(topics = "transaction_log", groupId = "demo-group")
    public void listenMainTopic(ConsumerRecord<String, String> record, Acknowledgment ack) {
        processMessage(record, ack);
    }

    @KafkaListener(topicPattern = "topic-retry-.*", groupId = "dynamic-group")
    public void listenRetryTopic(ConsumerRecord<String, String> record, Acknowledgment ack) {
        processMessage(record,ack);
    }

    public void processMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            log.info("Processing message from topic {}: {}", record.topic(), record.value());

            batchMessageService.receiveMessage(record.value());

            ack.acknowledge();
        } catch (Exception e) {
            int retryCount = retryHandlerService.getRetryCount(record.headers());

            log.error("Error processing message (retryCount={}): {}", retryCount, e.getMessage());
            retryHandlerService.handleError(record.value(), retryCount);
            ack.acknowledge();
        }
    }

}
