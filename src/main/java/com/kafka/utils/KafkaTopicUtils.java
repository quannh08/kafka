package com.kafka.utils;

import org.apache.kafka.clients.admin.*;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Component
public class KafkaTopicUtils {
    public static boolean topicExists(String bootstrapServers, String topicName) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(props)) {
            // Lấy danh sách topic
            Set<String> names = admin.listTopics().names().get();

            return names.contains(topicName); // kiểm tra topicName có trong list không
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void createTopic(String bootstrapServers, String topicName, int numPartitions, short replicationFactor) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
            admin.createTopics(Collections.singletonList(newTopic));
            System.out.println("Topic created: " + topicName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

