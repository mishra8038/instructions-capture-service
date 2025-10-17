package com.example.instructions.service;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.util.TradeTransformer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaListenerService {

    private final ObjectMapper objectMapper;
    private final TradeService tradeService;
    private final KafkaPublisher publisher;


    /**
     * Process inbound batch of trades. Note that the kafka listener is configured to use a default value but
     * looks up  the property value from application.yaml
     * @param message
     */
    @KafkaListener(topics = "${app.kafka.topics.inbound:instructions.inbound}",
            groupId = "${spring.kafka.consumer.group-id:instructions-capture-service}")
    public void onMessage(String message) {
        try {
            List<CanonicalTrade> inbound = objectMapper.readValue(message, new TypeReference<List<CanonicalTrade>>() {});
            for (CanonicalTrade ct : inbound) {
                CanonicalTrade transformed = TradeTransformer.transform(ct);
                tradeService.storeForAudit(transformed);
                publisher.publishCanonical(transformed);
            }
        } catch (Exception e) {
            log.error("Failed to process inbound batch", e);
        }
    }
}
