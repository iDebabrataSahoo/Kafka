package com.sanku.kafka.simpleproducerconsumer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfiguration {
    @Bean
    public NewTopic newTopic(){
    return new NewTopic("SimpleProducerConsumer", 3, (short) 1);
}

}
