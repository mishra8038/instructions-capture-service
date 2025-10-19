package com.example.instructions.stream.integration;

import com.example.instructions.InstructionsCaptureApplication;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
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
class KafkaStreamTransformerTestE2E {

    private static final String INBOUND_TOPIC = "instructions.inbound";
    private static final String OUTBOUND_TOPIC = "instructions.outbound";

    @Container
    static final org.testcontainers.kafka.KafkaContainer KAFKA = new org.testcontainers.kafka.KafkaContainer(DockerImageName.parse("apache/kafka-native").asCompatibleSubstituteFor("apache/kafka"))
                                                                    .withExposedPorts(9092)
                                                                    //.waitingFor(Wait.forHttp("/health").forPort(9092).forStatusCode(200))
                                                                    .withStartupTimeout(java.time.Duration.ofSeconds(40));

    @DynamicPropertySource
    static void springProps(DynamicPropertyRegistry r) {
        KAFKA.start();
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        r.add("spring.kafka.streams.application-id", () -> "instructions-transformer-it-" + UUID.randomUUID());
        // wire topic names if you externalize them
        r.add("app.kafka.topics.inbound", () -> INBOUND_TOPIC);
        r.add("app.kafka.topics.outbound", () -> OUTBOUND_TOPIC);
        // keep logs/data stable for tests
        r.add("spring.profiles.active", () -> "test");
    }

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

    @Test
    void roundTrip_transformer_masksAndUppercases() {
        // Arrange: produce two test trades to RAW_TOPIC
        try (KafkaProducer<String, String> producer = buildStringProducer()) {
            // NOTE: one CanonicalTrade per message; Streams consumes JsonSerde<CanonicalTrade>
            // StringSerializer will write UTF-8 bytes; JsonSerde happily deserializes the JSON bytes.
            producer.send(new ProducerRecord<>(INBOUND_TOPIC, "key-1", """
                    {
                      "account": "9876543210",
                      "security": "abc1234",
                      "type": "Buy",
                      "amount": 100000,
                      "timestamp": "2025-08-04T21:15:33Z"
                    }"""));
            producer.send(new ProducerRecord<>(INBOUND_TOPIC, "key-2", """
                    {
                      "account": "55500011119999",
                      "security": "xyz999",
                      "type": "SELL",
                      "amount": 5000,
                      "timestamp": "2025-08-05T10:00:00Z"
                    }"""));
            producer.flush();
        }

        // Act: consume from OUT_TOPIC and collect records for a short window
        List<String> values = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = buildStringConsumer("streams-it-consumer-" + UUID.randomUUID())) {
            consumer.subscribe(List.of(OUTBOUND_TOPIC));
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(15).toMillis();
            while (System.currentTimeMillis() < deadline && values.size() < 2) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                polled.forEach(rec -> values.add(rec.value()));
            }
        }

        // Assert: we saw at least two transformed messages
        assertTrue(values.size() >= 2, "Expected at least 2 outbound transformed records");

        // Assert: security uppercased & account masked (mask keeps last 4 digits)
        boolean sawUpperABC = values.stream().anyMatch(v -> v.contains("\"security\":\"ABC1234\""));
        boolean sawUpperXYZ = values.stream().anyMatch(v -> v.contains("\"security\":\"XYZ999\""));
        boolean sawMask3210 = values.stream().anyMatch(v -> v.contains("\"account\":\"******3210\""));
        boolean sawMask9999 = values.stream().anyMatch(v -> v.contains("\"account\":\"***********9999\"")); // 13-digit -> 9 stars + 4 digits

        assertTrue(sawUpperABC, "Expected ABC1234 uppercased in outbound");
        assertTrue(sawUpperXYZ, "Expected XYZ999 uppercased in outbound");
        assertTrue(sawMask3210, "Expected masked account ******3210 for 9876543210");
        assertTrue(sawMask9999, "Expected masked account ***********9999 for 55500011119999");
    }

    // ---------- Helpers ----------
    private static KafkaProducer<String, String> buildStringProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        return new KafkaProducer<>(p);
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
