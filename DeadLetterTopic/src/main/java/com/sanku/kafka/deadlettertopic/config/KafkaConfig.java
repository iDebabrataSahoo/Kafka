package com.sanku.kafka.deadlettertopic.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * KafkaConfig: Central configuration class for Kafka Producer, Consumer, and Topics.
 * 
 * WHAT DOES THIS CLASS DO?
 * - Configures how Java objects (UserDTO) are converted to JSON bytes when publishing to Kafka (Serializer).
 * - Configures how JSON bytes from Kafka are converted back to Java objects (Deserializer).
 * - Creates main topic and DLT topic programmatically if they don't exist yet on the Kafka broker.
 */
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.topic.name:user-events-topic}")
    private String topicName;

    // =========================================================================
    // 1. TOPIC DEFINITIONS (Automatic creation on Kafka startup)
    // =========================================================================

    /**
     * Creates the Main Kafka Topic programmatically if it doesn't already exist on Kafka cluster.
     * 
     * WHY USE THIS?
     * Instead of creating topics manually via command-line, Spring Admin Client automatically
     * creates the required topics when the application boots up.
     * 
     * - Partitions (3): Allows parallel message processing across multiple consumer instances.
     * - Replicas (1): Specifies copy count across brokers (1 for single-node local testing).
     */
    @Bean
    public NewTopic mainTopic() {
        return TopicBuilder.name(topicName)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Creates the Dead Letter Topic (DLT) programmatically.
     * 
     * By default, Spring `@RetryableTopic` appends ".DLT" or "-dlt" to the original topic name.
     * Creating it explicitly guarantees partition alignment and proper retention configuration.
     */
    @Bean
    public NewTopic dltTopic() {
        return TopicBuilder.name(topicName + ".DLT")
                .partitions(3)
                .replicas(1)
                .build();
    }

    // =========================================================================
    // 2. PRODUCER CONFIGURATION
    // =========================================================================

    /**
     * Configures producer properties (Bootstrap server, Serializer classes).
     * 
     * WHY USE JacksonJsonSerializer?
     * Apache Kafka only stores raw byte arrays. To send Java DTO objects (like UserDTO), 
     * we use `StringSerializer` for message keys and `JacksonJsonSerializer` to convert 
     * Java objects into JSON byte arrays automatically.
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * KafkaTemplate provides high-level convenience methods for sending messages to Kafka topics.
     * 
     * WHY USE KafkaTemplate?
     * It abstracts low-level Kafka Producer APIs and seamlessly handles connection setup,
     * message batching, and async callbacks (`CompletableFuture`).
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // =========================================================================
    // 3. CONSUMER CONFIGURATION
    // =========================================================================

    /**
     * Configures consumer properties (Deserializer classes, Trusted Packages).
     * 
     * WHY USE JacksonJsonDeserializer & TRUSTED_PACKAGES?
     * When reading messages from Kafka, the consumer converts JSON bytes back into Java objects.
     * We specify `TRUSTED_PACKAGES` to prevent untrusted remote code execution security vulnerabilities
     * during deserialization.
     */
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        // Trust all classes in our DTO package for deserialization
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "com.sanku.kafka.deadlettertopic.dto");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * KafkaListenerContainerFactory builds the listener containers for @KafkaListener annotations.
     * 
     * WHY USE ConcurrentKafkaListenerContainerFactory?
     * It enables concurrent message processing using multithreading for @KafkaListener methods.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
