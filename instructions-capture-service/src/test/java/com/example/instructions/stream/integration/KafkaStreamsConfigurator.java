package com.example.instructions.stream.integration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

@Slf4j
public class KafkaStreamsConfigurator {

    @EventListener(ApplicationReadyEvent.class)
    public void logTopologyOnReady(StreamsBuilderFactoryBean factory) {
        var topology = factory.getTopology(); // may be null if not built yet
        if (topology != null) {
            System.out.println("=== Topology [from FactoryBean @EventListener] ===");
            System.out.println ("----------------------------------------------------------------------------------------Topology:" + topology.describe());
        }
    }

} // KafkaStreamsConfigurator
