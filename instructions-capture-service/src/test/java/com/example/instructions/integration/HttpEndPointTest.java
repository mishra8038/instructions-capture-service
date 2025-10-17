package com.example.instructions.integration;

import com.example.instructions.InstructionsCaptureApplication;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = InstructionsCaptureApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class CsvUploadEndToEndIT {

    @Container static final org.testcontainers.kafka.KafkaContainer KAFKA = new org.testcontainers.kafka.KafkaContainer(DockerImageName.parse("apache/kafka-native").asCompatibleSubstituteFor("apache/kafka"));

    @DynamicPropertySource
    static void registerKafka(DynamicPropertyRegistry registry) {
        KAFKA.start();
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("app.kafka.topics.inbound", () -> "instructions.inbound");
        registry.add("app.kafka.topics.outbound", () -> "instructions.outbound");
        // keep the app profile predictable for tests
        registry.add("spring.profiles.active", () -> "test");
    }

    @LocalServerPort int port;

    @Test
    void uploadCsv_viaHttpClient_andAssertOutboundKafka() throws Exception {
        // --- Arrange test CSV (2 rows) ---
        String csv = """
            account,security,type,amount,timestamp
            9876543210,abc1234,B,100000,2025-08-04T21:15:33Z
            55500011119999,xyz999,SELL,5000,2025-08-05T10:00:00Z
            """.trim();

        // --- Send multipart/form-data to the Spring MVC endpoint ---
        String boundary = newBoundary();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/trades/upload/csv"))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(multipartBody(boundary, "file", "sample.csv", "text/csv", csv.getBytes(StandardCharsets.UTF_8)))
                .build();

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "CSV upload should return 200 OK");

        // --- Consume outbound topic to validate publish/transform path ---
        var consumer = buildStringConsumer("csv-it-consumer-" + UUID.randomUUID());
        try (consumer) {
            consumer.subscribe(List.of("instructions.outbound"));

            // Allow the app to publish; poll for a few seconds
            var records = consumer.poll(Duration.ofSeconds(10));
            assertTrue(records.count() >= 2, "Expected at least 2 outbound records after CSV upload");

            // Optional: spot-check transformation (masked account, uppercased security)
            //boolean sawMasked = records.records("instructions.outbound").stream().map(cr -> cr.value()).anyMatch(v -> v.contains("ABC1234") || v.contains("XYZ999"));
            //assertTrue(polled.count() >= 2, "Expected at least 2 outbound records after CSV upload");

            // Flatten all record values from all partitions
            boolean sawMasked = false;
            ConsumerRecords<String, String> polled = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, String> rec : polled) {
                String v = rec.value();
                if (v != null && (v.contains("ABC1234") || v.contains("XYZ999"))) { sawMasked = true; break; }
            }
            assertTrue(sawMasked, "Expected transformed securities (uppercased) in outbound payloads");
        }//ConsumerVerification
    }

    // ---------- Helpers ----------

    private static KafkaConsumer<String, String> buildStringConsumer(String groupId) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(groupId, "false", KAFKA.getBootstrapServers());
        Properties p = new Properties();
        p.putAll(props);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(p);
    }

    private static String newBoundary() {
        // simple, unique-ish boundary for the multipart payload
        return "----itBoundary" + ThreadLocalRandom.current().nextInt(1_000_000);
    }

    /**
     * Build a BodyPublisher for multipart/form-data with a single file part.
     * If you need multiple parts, extend this to accept a list of parts.
     */
    private static HttpRequest.BodyPublisher multipartBody( String boundary, String fieldName, String filename, String contentType, byte[] fileContent ) throws IOException {
        var byteArrays = new ArrayList<byte[]>();
        String preamble = "--" + boundary + "\r\n" +
                "Content-Disposition: form-data; name=\"" + fieldName + "\"; filename=\"" + filename + "\"\r\n" +
                "Content-Type: " + (contentType == null ? MediaType.APPLICATION_OCTET_STREAM_VALUE : contentType) + "\r\n" +
                "\r\n";
        byteArrays.add(preamble.getBytes(StandardCharsets.UTF_8));
        byteArrays.add(fileContent);
        byteArrays.add("\r\n".getBytes(StandardCharsets.UTF_8));

        String epilogue = "--" + boundary + "--\r\n";
        byteArrays.add(epilogue.getBytes(StandardCharsets.UTF_8));

        return HttpRequest.BodyPublishers.ofByteArrays(byteArrays);
    }
}