package com.sanku.kafka.deadlettertopic.service;

import com.sanku.kafka.deadlettertopic.dto.UserDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * ProducerService: Responsible for publishing messages to Kafka topics.
 * 
 * WHAT DOES THIS SERVICE DO?
 * - Accepts user event data (UserDTO).
 * - Uses Spring's `KafkaTemplate` to asynchronously publish events to the main Kafka topic.
 * - Handles publishing success or failure using non-blocking asynchronous callbacks (`CompletableFuture`).
 */
@Service
@Slf4j
public class ProducerService {

    /**
     * KafkaTemplate is injected by Spring Boot.
     * Key type: String (used for message partitioning key)
     * Value type: Object / UserDTO (payload converted to JSON byte array)
     */
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Target topic name configured in application.properties
     */
    @Value("${spring.kafka.topic.name:user-events-topic}")
    private String topicName;

    /**
     * Sends a UserDTO event to Kafka.
     * 
     * WHY ASYNCHRONOUS?
     * `kafkaTemplate.send(...)` is non-blocking. It sends the message in the background
     * and immediately returns a `CompletableFuture`. This keeps application throughput high 
     * because the web thread does not block waiting for Kafka network confirmation.
     * 
     * @param user User data payload to publish
     */
    public void sendMessage(UserDTO user) {
        log.info("Sending message to topic '{}': {}", topicName, user);

        // Send message using user.getUserId() as the message key for consistent partition routing
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topicName, user.getUserId(), user);

        // Attach success and failure callback handlers
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                // Success case: Log metadata such as partition and offset assigned by Kafka broker
                log.info("Message sent successfully! Topic: {}, Partition: {}, Offset: {}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                // Failure case: Broker connection failed, topic authorization failed, etc.
                log.error("Failed to send message [{}] due to error: {}", user, ex.getMessage(), ex);
            }
        });
    }
}
