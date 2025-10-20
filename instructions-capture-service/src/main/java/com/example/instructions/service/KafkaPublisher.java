package com.example.instructions.service;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.PlatformTrade;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Kafka publisher service, which can publish a canonical trade to a preconfigured Kafka topic.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topics.outbound:instructions.outbound}") private String outboundTopic;
    public void publishCanonical(CanonicalTrade trade) {
        try {
            String payload = objectMapper.writeValueAsString(trade);
            kafkaTemplate.send(outboundTopic, payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize trade", e);
        }
    }

    @Value("${app.kafka.topics.inbound:instructions.inbound}") private String inboundTopic;
    public void publishToInbound (CanonicalTrade ct) {
        try {
            String payload = objectMapper.writeValueAsString(ct);
            kafkaTemplate.send(inboundTopic, payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize platform trade", e);
        }
    }
} //KafkaPublisher
