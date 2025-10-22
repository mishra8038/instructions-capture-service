package com.example.instructions.service.integration;

import com.example.instructions.InstructionsCaptureApplication;
import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.service.KafkaPublisher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = InstructionsCaptureApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Testcontainers
@ActiveProfiles("test")
class KafkaRoundTripIT {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native").asCompatibleSubstituteFor("apache/kafka"))
                    .withExposedPorts(9092)
                    .waitingFor(Wait.forListeningPort())
                    .withStartupTimeout(java.time.Duration.ofSeconds(40));

    @Autowired
    KafkaPublisher kafkaPublisher;

    @DynamicPropertySource
    static void registerKafka(DynamicPropertyRegistry registry) {
        KAFKA.start();
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("app.kafka.topics.inbound", () -> "instructions.inbound");
        registry.add("app.kafka.topics.outbound", () -> "instructions.outbound");
    }

    @LocalServerPort int port;

    @Test
    @Order(1)
    void postJsonThroughRest_triggersOutbound() throws Exception {
        String json = """
            {
              "platform_id": "ACCT123",
              "trade": {
                "account": "9876543210",
                "security": "abc1234",
                "type": "B",
                "amount": 100000,
                "timestamp": "2025-08-04T21:15:33Z"
              }
            }
            """;

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/trade/instructions"))
                .header("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        client.send(req, HttpResponse.BodyHandlers.ofString());

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(java.util.List.of("instructions.outbound"));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertTrue(records.count() > 0, "Expected outbound messages for HTTP Request");
        }
    }


    @Test
    @Order(2)
    void publishJsonOnTopic_triggersOutbound() throws Exception {
        CanonicalTrade ct = CanonicalTrade.builder()
                .account("9876543210")
                .security("abc1234")
                .type("Buy")
                .amount(100000)
                .timestamp(OffsetDateTime.parse("2025-08-04T21:15:33Z"))
                .build();

        kafkaPublisher.publishCanonicalToInbound(ct);

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(java.util.List.of("instructions.outbound"));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertTrue(records.count() > 0, "Expected outbound messages for JSON");
        }
    }
}
