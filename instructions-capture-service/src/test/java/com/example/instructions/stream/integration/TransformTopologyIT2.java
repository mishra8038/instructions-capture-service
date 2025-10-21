package com.example.instructions.stream.integration;

import com.example.instructions.InstructionsCaptureApplication; // or your @SpringBootApplication class
import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.service.KafkaPublisher;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end roundtrip test:
 *  - Starts a Kafka broker (Testcontainers)
 *  - Bootstraps the Spring Kafka Streams topology
 *  - Produces CanonicalTrade JSON to instructions.inbound
 *  - Consumes transformed messages from instructions.outbound
 */
@SpringBootTest(classes = InstructionsCaptureApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE )// Streams only
@Profile("kstreams")
@Testcontainers
@Slf4j
class TransformTopologyIT2 {

    private static final String INBOUND_TOPIC = "instructions.inbound";
    private static final String OUTBOUND_TOPIC = "instructions.outbound";

    @Autowired KafkaPublisher kafkaPublisher;

    // Single-node Kafka for the test
    @Container
    static final org.testcontainers.kafka.KafkaContainer KAFKA = new org.testcontainers.kafka.KafkaContainer(
            DockerImageName.parse("apache/kafka-native")
            //DockerImageName.parse("confluentinc/cp-kafka:8.1.0")
            .asCompatibleSubstituteFor("apache/kafka"))
            .withExposedPorts(9092)
            .withCreateContainerCmdModifier(cmd -> {
                cmd.getHostConfig().withPortBindings(
                        new PortBinding(Ports.Binding.bindPort(9092), new ExposedPort(9092))
                );
            })
            .waitingFor(Wait.forListeningPort())
            .withStartupTimeout(java.time.Duration.ofSeconds(20));

    @DynamicPropertySource
    static void springProps(DynamicPropertyRegistry r) {
        //KAFKA.start();
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        r.add("spring.kafka.streams.application-id", () -> "instructions-transformer-it-" + UUID.randomUUID());
        // wire topic names if you externalize them
        r.add("app.kafka.topics.inbound", () -> INBOUND_TOPIC);
        r.add("app.kafka.topics.outbound", () -> OUTBOUND_TOPIC);
        // keep logs/data stable for tests
        r.add("spring.profiles.active", () -> "kstreams");
    }

    @BeforeAll
    static void createTopics() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            var topics = List.of(
                    new NewTopic(INBOUND_TOPIC, 3, (short) 1),
                    new NewTopic(OUTBOUND_TOPIC, 3, (short) 1)
            );
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void roundTrip_masksAccount_and_UppercasesSecurity() {

        System.out.println ("Kafka container mapped port listening at: " + KAFKA.getMappedPort(9092));
        System.out.println ("Kafka Bootstrap Servers: " + KAFKA.getBootstrapServers());
        for (int i = 0; i < 10; i++) {
            CanonicalTrade ct = CanonicalTrade.builder()
                    .account("55500011110000"+i)
                    .security("xyz999-"+i)
                    .type("SELL")
                    .amount(5000)
                    .timestamp(OffsetDateTime.parse("2025-08-05T10:00:00Z"))
                    .build();
            kafkaPublisher.publishToInbound(ct);
        }

        // Act: consume transformed messages from outbound
        List<String> values = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = buildStringConsumer("streams-it-consumer-" + UUID.randomUUID())) {
            consumer.subscribe(List.of(OUTBOUND_TOPIC));
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
            while (System.currentTimeMillis() < deadline && values.size() < 2) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                System.out.println ("Polled:" + polled);
                polled.forEach(rec -> values.add(rec.value()));
                System.out.println ("Polled:" + polled);
            }//while
        }//consumer

        // Assert: we saw at least two records
        assertTrue(values.size() >= 2, "Expected at least 2 outbound transformed records");

        // Assert: transformation effects
        boolean sawUpperABC = values.stream().anyMatch(v -> v.contains("\"security\":\"ABC1234\""));
        boolean sawUpperXYZ = values.stream().anyMatch(v -> v.contains("\"security\":\"XYZ999\""));
        boolean sawMask3210 = values.stream().anyMatch(v -> v.contains("\"account\":\"******3210\""));
        boolean sawMask9999 = values.stream().anyMatch(v -> v.contains("\"account\":\"***********9999\""));

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
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"); // if EOSv2 is on
        return new KafkaConsumer<>(p);
    }
}