package com.sanku.kafka.deadlettertopic.controller;

import com.sanku.kafka.deadlettertopic.dto.UserDTO;
import com.sanku.kafka.deadlettertopic.service.ProducerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;
import java.util.stream.IntStream;

/**
 * ProducerController: REST API Controller to trigger message publishing.
 * 
 * WHAT DOES THIS CONTROLLER DO?
 * Exposes REST HTTP endpoints allowing developers to easily trigger:
 * 1. Valid Kafka messages (processed normally by consumer).
 * 2. Invalid Kafka messages (triggers retries & moves to DLT).
 * 3. Custom JSON payload publishing.
 * 4. Bulk message testing.
 */
@RestController
@RequestMapping("/api/kafka")
@Slf4j
@Tag(name = "Kafka Producer Controller", description = "Endpoints for publishing messages to Kafka topics to test normal vs DLT handling")
public class ProducerController {

    @Autowired
    private ProducerService producerService;

    /**
     * Endpoint 1: Publish a VALID message.
     * 
     * URL: POST http://localhost:8080/api/kafka/publish/valid
     */
    @Operation(
        summary = "Publish a Valid Message", 
        description = "Publishes a valid UserDTO (with email containing '@'). Consumer will process it successfully without DLT."
    )
    @ApiResponse(responseCode = "200", description = "Message published successfully")
    @PostMapping("/publish/valid")
    public ResponseEntity<String> sendValidMessage() {
        UserDTO validUser = UserDTO.builder()
                .userId("USER-" + UUID.randomUUID().toString().substring(0, 8))
                .name("John Doe")
                .email("john.doe@example.com") // Contains '@' -> VALID
                .build();

        producerService.sendMessage(validUser);
        return ResponseEntity.ok("SUCCESS: Published VALID message to Kafka: " + validUser);
    }

    /**
     * Endpoint 2: Publish an INVALID message (Triggers DLT flow!).
     * 
     * URL: POST http://localhost:8080/api/kafka/publish/invalid
     */
    @Operation(
        summary = "Publish an Invalid Message (Triggers DLT)", 
        description = "Publishes an invalid UserDTO (email missing '@'). Consumer will throw exception, retry 3 times with backoff delay, and route to Dead Letter Topic."
    )
    @ApiResponse(responseCode = "200", description = "Invalid message published, DLT retry flow triggered")
    @PostMapping("/publish/invalid")
    public ResponseEntity<String> sendInvalidMessage() {
        UserDTO invalidUser = UserDTO.builder()
                .userId("USER-" + UUID.randomUUID().toString().substring(0, 8))
                .name("Bad Payload User")
                .email("bad-email-no-at-sign") // Missing '@' -> INVALID
                .build();

        producerService.sendMessage(invalidUser);
        return ResponseEntity.ok("TRIGGERED DLT: Published INVALID message to Kafka. Check logs for 3 retry attempts & DLT handler routing: " + invalidUser);
    }

    /**
     * Endpoint 3: Publish a CUSTOM user payload.
     * 
     * URL: POST http://localhost:8080/api/kafka/publish/custom
     */
    @Operation(
        summary = "Publish Custom User JSON Payload", 
        description = "Accepts a custom JSON body and publishes it to the Kafka topic."
    )
    @ApiResponse(responseCode = "200", description = "Custom message published successfully")
    @PostMapping("/publish/custom")
    public ResponseEntity<String> sendCustomMessage(@RequestBody UserDTO user) {
        if (user.getUserId() == null) {
            user.setUserId("USER-" + UUID.randomUUID().toString().substring(0, 8));
        }
        producerService.sendMessage(user);
        return ResponseEntity.ok("Published custom message: " + user);
    }

    /**
     * Endpoint 4: Bulk test with both valid and invalid records.
     * 
     * URL: POST http://localhost:8080/api/kafka/publish/bulk/5
     */
    @Operation(
        summary = "Publish Bulk Messages", 
        description = "Sends N messages (odd numbers valid, even numbers invalid) to prove DLT messages do not block valid messages."
    )
    @ApiResponse(responseCode = "200", description = "Bulk publishing completed")
    @PostMapping("/publish/bulk/{count}")
    public ResponseEntity<String> sendBulkMessages(@PathVariable int count) {
        IntStream.rangeClosed(1, count).forEach(i -> {
            boolean isValid = i % 2 != 0; // Odd numbers valid, Even numbers invalid
            UserDTO user = UserDTO.builder()
                    .userId("BULK-USER-" + i)
                    .name("User Number " + i)
                    .email(isValid ? "user" + i + "@example.com" : "invaliduser" + i + "domain.com")
                    .build();

            producerService.sendMessage(user);
        });

        return ResponseEntity.ok("Bulk publishing triggered for " + count + " messages (odd = valid, even = invalid/DLT).");
    }
}
