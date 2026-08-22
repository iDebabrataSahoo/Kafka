package com.sanku.kafka.simpleproducerconsumer.service;

import com.sanku.kafka.simpleproducerconsumer.dto.UserDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ConsumerService {


    @KafkaListener(topics = "${spring.kafka.topicName}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeEvents(UserDTO msg) {
        log.info("consumer consume the events {} ", msg.toString());
    }
}