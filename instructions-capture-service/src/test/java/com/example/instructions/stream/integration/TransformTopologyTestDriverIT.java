package com.example.instructions.stream.integration;

import com.example.instructions.InstructionsCaptureApplication;
import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.util.TradeTransformer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.OffsetDateTime;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = InstructionsCaptureApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE) // Streams only; no web required
@Slf4j
@Profile("kstreams")
public class TransformTopologyTestDriverIT {

    private static final String IN_TOPIC  = "instructions.inbound";
    private static final String OUT_TOPIC = "instructions.outbound";

    @Test
    void topology_masksAccount_and_uppercasesSecurity() {
        // Build a minimal topology in-memory (no Spring context required)
        StreamsBuilder builder = new StreamsBuilder();
        var tradeSerde = new JsonSerde<>(CanonicalTrade.class);

        builder.stream(IN_TOPIC, Consumed.with(Serdes.String(), tradeSerde))
                //.transformValues ((t)->TradeTransformer.transform(t))
                .mapValues((t)->TradeTransformer.transform(t))
                .to(OUT_TOPIC, Produced.with(Serdes.String(), tradeSerde));

        Topology topology = builder.build();

        // Standard test driver properties (dummy bootstrap is fine)
        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "transform-topology-ttd-it");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            // Create test topics
            TestInputTopic<String, CanonicalTrade> in = driver.createInputTopic(IN_TOPIC, new StringSerializer(), tradeSerde.serializer());

            TestOutputTopic<String, CanonicalTrade> out = driver.createOutputTopic(OUT_TOPIC, new StringDeserializer(), tradeSerde.deserializer());

            // Push two input records
            CanonicalTrade t1 = CanonicalTrade.builder()
                    .account("9876543210")
                    .security("abc1234")
                    .type("B")
                    .amount(100000)
                    .timestamp(OffsetDateTime.parse("2025-08-04T21:15:33Z"))
                    .build();

            CanonicalTrade t2 = CanonicalTrade.builder()
                    .account("55500011119999")
                    .security("xyz999")
                    .type("SELL")
                    .amount(5000)
                    .timestamp(OffsetDateTime.parse("2025-08-05T10:00:00Z"))
                    .build();

            in.pipeInput("k1", t1);
            in.pipeInput("k2", t2);

            // Read and assert outputs
            assertFalse(out.isEmpty(), "Expected transformed records on outbound topic");

            CanonicalTrade o1 = out.readValue();
            CanonicalTrade o2 = out.readValue();

            // Order depends on processing; normalize assertions
            assertTrue(
                    (o1.getSecurity().equals("ABC1234") && o1.getAccount().endsWith("3210")) ||
                            (o2.getSecurity().equals("ABC1234") && o2.getAccount().endsWith("3210")),
                    "Expected security ABC1234 and masked account ******3210"
            );
            assertTrue(
                    (o1.getSecurity().equals("XYZ999") && o1.getAccount().endsWith("9999")) ||
                            (o2.getSecurity().equals("XYZ999") && o2.getAccount().endsWith("9999")),
                    "Expected security XYZ999 and masked account ***********9999"
            );

            // Stronger mask checks (stars + last 4)
            String a1 = o1.getAccount();
            String a2 = o2.getAccount();
            assertTrue(a1.matches("\\*+\\d{4}") && a2.matches("\\*+\\d{4}"),
                    "Accounts should be masked as stars + last 4 digits");
        }
    }
}

