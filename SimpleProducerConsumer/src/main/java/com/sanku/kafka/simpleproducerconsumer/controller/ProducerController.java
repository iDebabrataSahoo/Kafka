package com.sanku.kafka.simpleproducerconsumer.controller;

import com.sanku.kafka.simpleproducerconsumer.dto.UserDTO;
import com.sanku.kafka.simpleproducerconsumer.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.stream.IntStream;

@RestController
@RequestMapping("/publish")
public class ProducerController {
    @Autowired
    public ProducerService producerService;

    @PostMapping("/{message}")
    public String sendMessage(@PathVariable String message) {
        // producerService.sendMessage(message);
        return "message is published .. that is -> " + message;
    }

    @PostMapping("/bulk/{number}")
    public String bulkSendMessage(@PathVariable int number) {
        IntStream.rangeClosed(1, number).forEach(i -> producerService.sendMessage(
                new UserDTO("Name " + i, "Name " + i + "@gmail.com")
        ));
        return "message are published ..";
    }
}
