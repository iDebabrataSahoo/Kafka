package com.sanku.kafka.deadlettertopic;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * DeadLetterTopicApplication
 * 
 * Entry point for the Spring Boot Kafka Dead Letter Topic (DLT) Demonstration.
 * 
 * To test:
 * 1. Start Apache Kafka broker locally on localhost:9092 (or via Docker Compose).
 * 2. Run this application.
 * 3. Use cURL or Postman to send requests to http://localhost:8080/api/kafka/publish/...
 */
@SpringBootApplication
public class DeadLetterTopicApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeadLetterTopicApplication.class, args);
    }

}
