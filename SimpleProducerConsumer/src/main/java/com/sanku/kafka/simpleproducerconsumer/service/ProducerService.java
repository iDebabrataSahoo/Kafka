package com.sanku.kafka.simpleproducerconsumer.service;

import com.sanku.kafka.simpleproducerconsumer.dto.UserDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class ProducerService {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Value("${spring.kafka.topicName}")
    private String topicName;

    public void sendMessage(UserDTO message) {
        CompletableFuture<SendResult<String, Object>> sendResultCompletableFuture = kafkaTemplate.send(topicName, message);
        sendResultCompletableFuture.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        message + "] due to : " + ex.getMessage());
            }
        });
    }
}
