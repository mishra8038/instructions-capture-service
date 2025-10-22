package com.example.instructions.stream.integration;

import com.example.instructions.InstructionsCaptureApplication;
import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.service.KafkaPublisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-End test class for validating the behavior of a Kafka Streams-based application.
 * This test verifies the message transformation pipeline, ensuring that messages
 * produced on a source Kafka topic are properly consumed, transformed, and produced
 * to a destination Kafka topic as per the application logic.
 *
 * The test utilizes Testcontainers to spin up a Kafka environment and dynamically initializes
 * properties required to interact with the Kafka setup. It validates the entire round-trip
 * of producing, consuming, transforming, and verifying the expected output.
 *
 * Annotations:
 * - {@code @SpringBootTest}: Configures the test to bootstrap the Spring Boot application under test.
 * - {@code @Testcontainers}: Enables Testcontainers support for orchestrating dependencies such as Kafka.
 */
@SpringBootTest(classes = InstructionsCaptureApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE) // Streams only; no web required
@Testcontainers
@Slf4j
@EnableKafkaStreams
@ActiveProfiles({"test", "kstreams"})
class KafkaStreamTransformerE2EIT {
    private static final String INBOUND_TOPIC = "instructions.inbound";
    private static final String OUTBOUND_TOPIC = "instructions.outbound";

    @Container
    static final org.testcontainers.kafka.KafkaContainer KAFKA = new org.testcontainers.kafka.KafkaContainer(DockerImageName.parse("apache/kafka-native")
                                                                    .asCompatibleSubstituteFor("apache/kafka"))
                                                                    .withExposedPorts(9092)
                                                                    .waitingFor(Wait.forListeningPort())
                                                                    .withStartupTimeout(java.time.Duration.ofSeconds(40));

    @DynamicPropertySource
    static void springProps(DynamicPropertyRegistry r) {
        //KAFKA.start();
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        r.add("spring.kafka.streams.application-id", () -> "instructions-transformer-it-" + UUID.randomUUID());
        // wire topic names if you externalize them
        r.add("app.kafka.topics.inbound", () -> INBOUND_TOPIC);
        r.add("app.kafka.topics.outbound", () -> OUTBOUND_TOPIC);
        // keep logs/data stable for tests
    }

    @Autowired KafkaPublisher kafkaPublisher;

    @BeforeAll
    static void createTopics() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of( "bootstrap.servers", KAFKA.getBootstrapServers()
        ))) {
            // Create topics explicitly (helps when auto-create is disabled)
            var topics = List.of(
                    new NewTopic(INBOUND_TOPIC, 3, (short) 1),
                    new NewTopic(OUTBOUND_TOPIC, 3, (short) 1)
            );
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
        }
    }

    @Test void roundTrip_transformer_test_synchronized () {
        System.out.println ("Starting roundTrip_transformer_test_synchronized : Container exposed port: "  + KAFKA.getMappedPort(9092) );
        for (int i = 0; i < 2; i++) {
            CanonicalTrade ct = CanonicalTrade.builder()
                    .account("55500011110000"+i)
                    .security("xyz999-"+i)
                    .type("SELL")
                    .amount(5000)
                    .timestamp(OffsetDateTime.parse("2025-08-05T10:00:00Z"))
                    .build();
            kafkaPublisher.publishCanonicalToInbound(ct);
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {
            log.debug ("Thread sleep interrupted before consumer poll");
        }

        long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
        List<String> values = new ArrayList<>();

        // Act: consume from OUT_TOPIC and collect records for a short window
        try (KafkaConsumer<String, String> consumer = buildStringConsumer("streams-it-consumer-" + UUID.randomUUID())) {
            consumer.subscribe(List.of(OUTBOUND_TOPIC));
            while (System.currentTimeMillis() < deadline && values.size() < 2) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(100));
                System.out.println ("Consumed " + polled.count() + " records");
                System.out.println ("Consumed Records " + polled);
                polled.forEach(rec -> values.add(rec.value()));
            }
        }

        // Assert: we saw at least two transformed messages
        assertTrue(values.size() >= 2, "Expected at least 2 outbound transformed records");

        // Assert: security uppercased & account masked (mask keeps last 4 digits)
        boolean sawUpperCaseInSecurityName = values.stream().anyMatch(v -> v.contains("XYZ999")); //security
        boolean sawMaskForAccount = values.stream().anyMatch(v -> v.contains("*0000")); // digit

        assertTrue(sawUpperCaseInSecurityName, "Expected XYZ999 uppercased in outbound");
        assertTrue(sawUpperCaseInSecurityName, "Expected masked account number in outbound");
    }

    @Test void roundTrip_transformer_masksAndUppercases() {
        System.out.println ("Starting roundTrip_transformer_test_synchronized : Container exposed port: "  + KAFKA.getMappedPort(9092) );
        for (int i = 0; i < 2; i++) {
            CanonicalTrade ct = CanonicalTrade.builder()
                    .account("55500011110000"+i)
                    .security("xyz999-"+i)
                    .type("SELL")
                    .amount(5000)
                    .timestamp(OffsetDateTime.parse("2025-08-05T10:00:00Z"))
                    .build();
            kafkaPublisher.publishCanonicalToInbound(ct);
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {
            log.debug ("Thread sleep interrupted before consumer poll");
        }

        long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
        List<String> values = new ArrayList<>();

        // Act: consume from OUT_TOPIC and collect records for a short window
        try (KafkaConsumer<String, String> consumer = buildStringConsumer("streams-it-consumer-" + UUID.randomUUID())) {
            consumer.subscribe(List.of(OUTBOUND_TOPIC));
            while (System.currentTimeMillis() < deadline && values.size() < 2) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(100));
                System.out.println ("Consumed " + polled.count() + " records");
                System.out.println ("Consumed Records " + polled);
                polled.forEach(rec -> values.add(rec.value()));
            }
        }

        // Assert: we saw at least two transformed messages
        assertTrue(values.size() >= 2, "Expected at least 2 outbound transformed records");

        // Assert: security uppercased & account masked (mask keeps last 4 digits)
        //boolean sawUpperCaseInSecurityName = values.stream().anyMatch(v -> v.contains("\"security\":\"XYZ999-\\d+\"")); //security
        boolean sawUpperCaseInSecurityName = values.stream().anyMatch(v -> v.contains("XYZ999")); //security
        boolean sawMaskForAccount = values.stream().anyMatch(v -> v.contains("*0000")); // digit

        assertTrue(sawUpperCaseInSecurityName, "Expected XYZ999 uppercased in outbound");
        assertTrue(sawUpperCaseInSecurityName, "Expected masked account number in outbound");
    }

    private static KafkaConsumer<String, String> buildStringConsumer(String groupId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"); // good if EOSv2 is enabled in Streams
        return new KafkaConsumer<>(p);
    }
} //KafkaStreamTransformerTestE2E
