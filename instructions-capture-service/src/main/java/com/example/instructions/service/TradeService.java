package com.example.instructions.service;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.PlatformTrade;
import com.example.instructions.util.TradeTransformer;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Trade service which processes trade instructions and publishes them to Kafka.
 *
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TradeService {

    private final KafkaPublisher kafkaPublisher;

    private final Map<String, CanonicalTrade> auditStore = new ConcurrentHashMap<>();

    @PostConstruct void init() { log.info("TradeService initialized"); }

    public List<CanonicalTrade> processBulkUploadTradeInstrutionsCsv (MultipartFile file) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8));
             CSVParser parser = CSVFormat.DEFAULT
                     .withFirstRecordAsHeader()
                     .withTrim()
                     .parse(reader)) {
            //processing in context of CSVParser
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
                log.debug ("Publishing Canonical Trade on Kafka Inbound.", transformed);
                kafkaPublisher.publishCanonical(transformed);
                results.add(transformed);
            } // end of csv record processing for loop
            return results;
        } catch (IOException e) {
            throw new RuntimeException("CSV parse failed: " + e.getMessage(), e);
        }
    } // end processCsv

    public List<CanonicalTrade> processBulkUploadTradeInstrutionsJSON (MultipartFile file) {
        String line;
        ObjectMapper objectMapper = new ObjectMapper();
        List<CanonicalTrade> results = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))) { // Specify charset for InputStreamReader
            System.out.println("Enter text (type 'exit' to quit):");
            while ((line = reader.readLine()) != null) {
                PlatformTrade platformTrade = objectMapper.readValue(line, PlatformTrade.class);
                CanonicalTrade transformed = TradeTransformer.transform(platformTrade.getTrade());
                log.debug ("Publishing Canonical Trade on Kafka Inbound.", transformed);
                kafkaPublisher.publishCanonical(transformed);
                results.add(transformed);
            }
            return results;
        } catch (FileNotFoundException e) {
            log.error("Unable to read uploaded json file", e);
            throw new RuntimeException("Unable to read json file", e);
        } catch (IOException e) {
            log.error("IO Exception occurred while transforming json file", e);
            throw new RuntimeException("IO Exception occurred while transforming json file", e);
        }
    } // end processJson

    public CanonicalTrade processTradeInstruction (@Valid PlatformTrade payload) {
        CanonicalTrade transformed = TradeTransformer.transform(payload.getTrade());
        log.debug ("Publishing Canonical Trade on Kafka Inbound.", transformed);
        kafkaPublisher.publishCanonical(transformed);
        return transformed;
    }

}
