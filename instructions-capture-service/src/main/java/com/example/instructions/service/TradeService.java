package com.example.instructions.service;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.PlatformTrade;
import com.example.instructions.util.TradeTransformer;
import jakarta.annotation.PostConstruct;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class TradeService {

    private final KafkaPublisher kafkaPublisher;

    private final Map<String, CanonicalTrade> auditStore = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {
        log.info("TradeService initialized");
    }

    public List<CanonicalTrade> processCsv(MultipartFile file) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8));
             CSVParser parser = CSVFormat.DEFAULT
                     .withFirstRecordAsHeader()
                     .withTrim()
                     .parse(reader)) {

            List<CanonicalTrade> results = new ArrayList<>();
            for (CSVRecord r : parser) {
                CanonicalTrade raw = CanonicalTrade.builder()
                        .account(r.get("account"))
                        .security(r.get("security"))
                        .type(r.get("type"))
                        .amount(Long.parseLong(r.get("amount")))
                        .timestamp(OffsetDateTime.parse(r.get("timestamp")))
                        .build();

                CanonicalTrade transformed = TradeTransformer.transform(raw);
                storeForAudit(transformed);
                kafkaPublisher.publishCanonical(transformed);
                results.add(transformed);
            }
            return results;
        } catch (Exception e) {
            throw new RuntimeException("CSV parse failed: " + e.getMessage(), e);
        }
    }

    public CanonicalTrade processJson(@Valid PlatformTrade payload) {
        CanonicalTrade transformed = TradeTransformer.transform(payload.getTrade());
        storeForAudit(transformed);
        kafkaPublisher.publishCanonical(transformed);
        return transformed;
    }

    public void storeForAudit(CanonicalTrade trade) {
        String key = trade.getSecurity() + ":" + trade.getTimestamp();
        auditStore.put(key, trade);
    }

    @Cacheable(cacheNames = "audit", key = "#key")
    public Optional<CanonicalTrade> getFromAudit(String key) {
        return Optional.ofNullable(auditStore.get(key));
    }
}
