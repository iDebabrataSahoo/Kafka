package com.sanku.kafka.deadlettertopic.service;

import com.sanku.kafka.deadlettertopic.dto.UserDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

/**
 * ConsumerService: Main Kafka Listener and Dead Letter Topic (DLT) Handler.
 * 
 * ========================================================================================
 * WHAT IS A DEAD LETTER TOPIC (DLT)?
 * ========================================================================================
 * In event-driven architectures, consumers sometimes fail to process incoming messages due to:
 * 1. Data errors (e.g., malformed JSON, missing mandatory fields, invalid data types).
 * 2. Transient failures (e.g., temporary database disconnect, downstream service rate limit).
 * 
 * Without a DLT:
 * If a consumer crashes on a bad message ("poison pill"), Kafka keeps re-delivering that exact same
 * bad message forever. This blocks processing of all subsequent valid messages in the partition!
 * 
 * With a DLT:
 * 1. The consumer retries processing the message a specified number of times (`attempts = "3"`).
 * 2. It waits between attempts using a backoff delay (`delay = 2000ms`, `multiplier = 2.0`).
 * 3. If processing STILL fails after max attempts, the message is automatically moved to the DLT!
 * 4. Main partition consumer continues processing new messages without getting blocked!
 * ========================================================================================
 */
@Service
@Slf4j
public class ConsumerService {

    /**
     * Main Kafka Consumer with Automatic Retry and DLT Routing.
     * 
     * ANNOTATION BREAKDOWN:
     * 
     * 1. @KafkaListener:
     *    - topics: Listens to topic name specified in properties ("user-events-topic").
     *    - groupId: Group ID for offset management.
     * 
     * 2. @RetryableTopic:
     *    - attempts = "3": Tries processing 3 times (1 main attempt + 2 retries).
     *    - backOff = @BackOff(delay = 2000, multiplier = 2.0):
     *        * Attempt 1: Immediate
     *        * Attempt 2: Waits 2 seconds (2000ms)
     *        * Attempt 3: Waits 4 seconds (2000ms * 2.0)
     *    - dltStrategy = DltStrategy.FAIL_ON_ERROR:
     *        * When max attempts are exhausted, route the message to the Dead Letter Topic.
     *    - include = { RuntimeException.class, IllegalArgumentException.class }:
     *        * Triggers retry mechanism when these exception types are thrown.
     * 
     * WHY USE BACKOFF DELAY?
     * Exponential backoff gives downstream services (like database or external REST APIs) 
     * time to recover from temporary glitches before retrying, avoiding "thundering herd" issues.
     */
    @RetryableTopic(
            attempts = "3",
            backOff = @BackOff(delay = 2000, multiplier = 2.0),
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = { RuntimeException.class, IllegalArgumentException.class }
    )
    @KafkaListener(topics = "${spring.kafka.topic.name:user-events-topic}", groupId = "${spring.kafka.consumer.group-id:user-events-consumer-group}")
    public void consumeUserEvent(
            UserDTO user,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("--> [MAIN CONSUMER] Processing message from Topic '{}' [Partition: {}, Offset: {}]: {}", 
                topic, partition, offset, user);

        // =====================================================================
        // SIMULATED BUSINESS VALIDATION
        // =====================================================================
        // Rule: Email must exist and must contain the '@' symbol.
        if (user.getEmail() == null || !user.getEmail().contains("@")) {
            log.error("--> [MAIN CONSUMER] Validation Failed! Invalid email '{}' for User ID '{}'. Throwing exception to trigger retry...",
                    user.getEmail(), user.getUserId());
            
            // Throwing exception triggers @RetryableTopic retry mechanism!
            throw new IllegalArgumentException("Invalid email format received: " + user.getEmail());
        }

        // Processing success logic
        log.info("--> [MAIN CONSUMER] SUCCESS! User '{}' ({}) processed successfully.", user.getName(), user.getEmail());
    }

    /**
     * Dead Letter Topic (DLT) Handler Method.
     * 
     * ANNOTATION BREAKDOWN:
     * 
     * @DltHandler:
     * Marks this method to handle messages routed to the DLT after all retry attempts fail.
     * 
     * WHAT HAPPENS HERE IN PRODUCTION?
     * When a poison pill message reaches here:
     * 1. Log the failure details and error stacktrace.
     * 2. Store the failed message payload into a database ("dead_letters" table) for audit/inspection.
     * 3. Send alerts to engineers (e.g., Slack, PagerDuty, email notification).
     * 4. Provide a UI/API for admins to correct the data and re-publish back to the main topic.
     * 
     * HEADERS ACCESSED:
     * - RECEIVED_TOPIC: The DLT topic name (e.g. "user-events-topic.DLT")
     * - ORIGINAL_TOPIC: The main topic where failure originally occurred
     * - EXCEPTION_MESSAGE: The exact exception message that caused the failure
     */
    @DltHandler
    public void handleDeadLetterTopic(
            UserDTO user,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String dltTopic,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(name = KafkaHeaders.ORIGINAL_TOPIC, required = false) String originalTopic,
            @Header(name = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage
    ) {
        log.warn("=========================================================================================");
        log.warn("--> [DLT HANDLER] CRITICAL: Message moved to Dead Letter Topic!");
        log.warn("--> DLT Topic        : {}", dltTopic);
        log.warn("--> Original Topic   : {}", originalTopic != null ? originalTopic : "user-events-topic");
        log.warn("--> DLT Offset       : {}", offset);
        log.warn("--> Failed User Payload: {}", user);
        log.warn("--> Exception Cause  : {}", exceptionMessage);
        log.warn("=========================================================================================");
        log.warn("--> Action Required: Investigate payload or fix code, then reprocess manually.");
    }
}