package com.example.instructions.service;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.util.TradeTransformer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Kafka listener service, which listens to the inbound Kafka topic and processes the inbound trades, into masked and canonicalized trades sent to the oubound topic.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@Profile("!kstreams")
public class KafkaListenerService {

    private final ObjectMapper objectMapper;
    private final KafkaPublisher publisher;

    /**
     * Process inbound batch of trades. Note that the kafka listener is configured to use a default value but
     * looks up  the property value from application.yaml
     * @param message
     */
    @KafkaListener(topics = "${app.kafka.topics.inbound:instructions.inbound}", groupId = "${spring.kafka.consumer.group-id:instructions-capture-service}")
    public void onMessage(String message) {
        try {
            List<CanonicalTrade> inbound = objectMapper.readValue(message, new TypeReference<List<CanonicalTrade>>() {});
            for (CanonicalTrade ct : inbound) {
                CanonicalTrade transformed = TradeTransformer.transform(ct);
                publisher.publishCanonical(transformed);
            }
        } catch (Exception e) {
            log.error("Failed to process inbound batch", e);
        }
    }
}
