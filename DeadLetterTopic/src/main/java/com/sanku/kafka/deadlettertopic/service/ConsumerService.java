package com.sanku.kafka.deadlettertopic.service;


import com.sanku.kafka.deadlettertopic.dto.UserDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.json.JsonParseException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
@Slf4j
public class ConsumerService {

    @RetryableTopic(attempts = "4")
    @KafkaListener(topics = "${spring.kafka.topicName}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeEvents(UserDTO user, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {
        try {

            String val = "@";
            if (!val.contains(user.getEmail())) {
                throw new RuntimeException("Invalid email ID received !");
            }
            log.info("Received: {} from {} offset {}", new ObjectMapper().writeValueAsString(user), topic, offset);

        } catch (JsonParseException e) {
            e.printStackTrace();
        }
    }

    @DltHandler
    public void consumeEventsForDLT(UserDTO user, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("Received: {} from {} offset {}", new ObjectMapper().writeValueAsString(user.getName()), topic, offset);
    }

}