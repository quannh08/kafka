package com.kafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;

@Service
@RequiredArgsConstructor
@Slf4j(topic = "RETRY-HANDLER-SERVICE")
public class RetryHandlerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AdminClient adminClient;

    public void handleError(String message, int retryCount) {
        String topicName = getRetryTopicName(retryCount);

        if (!topicExists(topicName)) {
            createTopic(topicName);
            log.info(" Created new topic: {}", topicName);
        }

        if (retryCount >= 4) {
            topicName = "topic-dlt";
            if (!topicExists(topicName)) createTopic(topicName);
        }

        // build record với header retry-count + 1
        ProducerRecord<String, Object> retryRecord = new ProducerRecord<>(topicName, message);
        retryRecord.headers().add(new RecordHeader("retry-count",
                String.valueOf(retryCount + 1).getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(retryRecord);
        log.warn("Sent message to retry topic {} with retryCount={}", topicName, retryCount + 1);
    }

    private String getRetryTopicName(int retryCount) {
        if (retryCount >= 4) return "topic-dlt";
        int delay = (retryCount+1) * 1000;
        return "topic-retry-" + delay;
    }

    private boolean topicExists(String topicName) {
        try {
            Set<String> topics = adminClient.listTopics().names().get();
            return topics.contains(topicName);
        } catch (Exception e) {
            log.error(" Error checking topic existence: {}", e.getMessage());
            return false;
        }
    }

    private void createTopic(String topicName) {
        NewTopic newTopic = TopicBuilder.name(topicName)
                .partitions(3)
                .replicas(2)
                .build();
        try {
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (Exception e) {
            log.error(" Failed to create topic {}: {}", topicName, e.getMessage());
        }
    }

    public int getRetryCount(Headers headers) {
        try {
            if (headers == null) return 0;
            var header = headers.lastHeader("retry-count");
            if (header == null) return 0;
            return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            return 0;
        }
    }
}

