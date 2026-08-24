package com.sanku.kafka.deadlettertopic.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * UserDTO (Data Transfer Object)
 * 
 * WHY USE DTO IN KAFKA?
 * In microservices and event-driven architectures, we don't send raw database entities 
 * over Kafka topics. Instead, we send Data Transfer Objects (DTOs). 
 * DTOs encapsulate only the relevant fields needed for communication, ensuring a clean API contract 
 * and protecting internal database schemas from exposure.
 * 
 * In this example:
 * - Valid message: email contains "@" (e.g. "john@example.com") -> Consumer processes successfully.
 * - Invalid message: email missing "@" (e.g. "invalidemail.com") -> Consumer throws exception, 
 *   triggers automatic retries, and eventually sends the message to the Dead Letter Topic (DLT).
 */
@Data                   // Generates getters, setters, toString(), equals(), and hashCode() methods automatically
@NoArgsConstructor      // Generates no-argument constructor (required by Jackson for JSON deserialization)
@AllArgsConstructor     // Generates constructor with all fields
@Builder                // Provides builder pattern for clean object creation
@Schema(description = "User Event Data Transfer Object sent via Kafka topic")
public class UserDTO {

    @Schema(description = "Unique Identifier for the User", example = "USER-101")
    private String userId;

    @Schema(description = "Full Name of the User", example = "John Doe")
    private String name;

    @Schema(description = "User Email Address (Must contain '@' for valid processing)", example = "john.doe@example.com")
    private String email;
}
