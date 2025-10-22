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
import org.springframework.beans.factory.annotation.Autowired;
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
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TradeService {

    private final KafkaPublisher kafkaPublisher;

    private final Map<String, CanonicalTrade> auditStore = new ConcurrentHashMap<>();

    @PostConstruct void init() { log.info("TradeService initialized"); }
    @Autowired ObjectMapper objectMapper;

    /**
     * Processes a CSV file containing bulk upload trade instructions, transforms
     * the records into CanonicalTrade objects, and publishes them to Kafka.
     *
     * @param file the CSV file containing trade instructions
     * @return a list of CanonicalTrade objects after processing and publishing
     * @throws RuntimeException if an error occurs during CSV parsing or file reading
     */
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
                kafkaPublisher.publishCanonicalToOutbound(transformed);
                results.add(transformed);
            } // end of csv record processing for loop
            return results;
        } catch (IOException e) {
            throw new RuntimeException("CSV parse failed: " + e.getMessage(), e);
        }
    } // processBulkUploadTradeInstrutionsCsv

    /**
     * Processes a JSON file containing bulk upload trade instructions, transforms
     * the records into CanonicalTrade objects, and publishes them to Kafka.
     *
     * @param file the JSON file containing trade instructions
     * @return a list of CanonicalTrade objects after processing and publishing
     * @throws RuntimeException if an error occurs during file reading or JSON parsing
     */
    public List<CanonicalTrade> processBulkUploadTradeInstrutionsJSON (MultipartFile file) {
        String line;
        List<CanonicalTrade> results = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))) { // Specify charset for InputStreamReader
            System.out.println("Enter text (type 'exit' to quit):");
            while ((line = reader.readLine()) != null) {
                PlatformTrade platformTrade = objectMapper.readValue(line, PlatformTrade.class);
                CanonicalTrade transformed = TradeTransformer.transform(platformTrade.getTrade());
                log.debug ("Publishing Canonical Trade on Kafka Inbound.", transformed);
                kafkaPublisher.publishCanonicalToOutbound(transformed);
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
    } // processBulkUploadTradeInstrutionsJSON

    /**
     * Processes a trade instruction by transforming a {@link PlatformTrade} payload into a
     * {@link CanonicalTrade} object and publishing it to Kafka.
     * @param payload the {@link PlatformTrade} object containing the trade instruction to be processed and transformed
     * @return the transformed {@link CanonicalTrade} object after processing
     */
    public CanonicalTrade processTradeInstruction (@Valid PlatformTrade payload) {
        CanonicalTrade transformed = TradeTransformer.transform(payload.getTrade());
        log.debug ("Publishing Canonical Trade on Kafka Inbound.", transformed);
        kafkaPublisher.publishCanonicalToOutbound(transformed);
        return transformed;
    } // processTradeInstruction
}
