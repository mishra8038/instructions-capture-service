package com.example.instructions.service.integration;

import com.example.instructions.InstructionsCaptureApplication;
import com.example.instructions.service.KafkaPublisher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = InstructionsCaptureApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Testcontainers
class CSVUploadE2EIT {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native").asCompatibleSubstituteFor("apache/kafka"))
                    .withExposedPorts(9092)
                    //.waitingFor(Wait.forHttp("/health").forPort(9092).forStatusCode(200))
                    .waitingFor(Wait.forListeningPort())
                    .withStartupTimeout(Duration.ofSeconds(40));

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
        // --- Arrange test CSV (2 rows) ---
        String csv = """
            account,security,type,amount,timestamp
            9876543210,abc1234,B,100000,2025-08-04T21:15:33Z
            55500011119999,xyz999,SELL,5000,2025-08-05T10:00:00Z
            """.trim();

        // --- Send multipart/form-data to the Spring MVC endpoint ---
        String boundary = newBoundary();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/trade/instructions"))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(multipartBody(boundary, "file", "sample.csv", "text/csv", csv.getBytes(StandardCharsets.UTF_8)))
                .build();

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "CSV upload should return 200 OK");

        //await processing before intiating the consumer.
        Thread.sleep(10000);

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
            // Allow the app to publish; poll for a few seconds
            assertTrue(records.count() >= 2, "Expected at least 2 outbound records after CSV upload");

            boolean validationWorking = false;
            boolean maskingWorking = false;
            for (ConsumerRecord<String, String> rec : records) {
                String v = rec.value();
                if (v != null && (v.contains("ABC1234") || v.contains("XYZ999"))) { validationWorking = true; break; }
                if (v != null && (v.contains("******3210") || v.contains("***********9999"))) { validationWorking = true; break; }
            }
            assertTrue(validationWorking, "Expected transformed uppercase securities value in outbound payloads");
        }
    }

    // ---------- Helpers ----------
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
