package com.example.instructions.stream;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.util.TradeTransformer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Service;

/**
 * Kafka stream transformer service, which transforms inbound trades into masked and canonicalized trades sent to the oubound topic.
 * This is a high throughput processor that replaces Kafka Listener if the dev profie kstreams is seected whie execution.
 */
@Slf4j
@Service
@Profile("kstreams")
@EnableKafkaStreams
public class KafkaStreamTransformer {

    @Value("${app.kafka.topics.inbound:instructions.inbound}")
    private String inboundTopic;

    @Value("${app.kafka.topics.outbound:instructions.outbound}")
    private String outTopic;

    @Value("${app.kafka.streams.cache.ttl-ms:600000}")
    private long ttlMs;

    @Value("${app.kafka.streams.cache.max-entries:250000}")
    private int maxEntries;

    @Value("${app.kafka.streams.cache.hmac-secret:dev-secret}")
    private String hmacSecret;

    private final JsonSerde<CanonicalTrade> tradeSerde = new JsonSerde<>(CanonicalTrade.class);

    /**
     * Transforms a Kafka stream of raw kafka trade data into a stream of canonicalized trade objects with deduplication,
     * transformation, and key partitioning for optimized downstream processing.
     * @param builder the StreamsBuilder used to construct Kafka Streams topology
     * @return a Kafka stream of canonicalized trades with keys based on a privacy-safe HMAC
     */
    @Bean
    public KStream<String, CanonicalTrade> tradeTransformPipeline(StreamsBuilder builder) {
        KStream<String, CanonicalTrade> source = builder.stream(inboundTopic, Consumed.with(Serdes.String(), tradeSerde));

        // 1) Convert nulls out;
        // 2) in-memory hot-window dedup + cache hit short-circuit;
        // 3) transform;
        // 4) key by privacy-safe HMAC account for partition affinity;
        // 5) publish
        KStream<String, CanonicalTrade> transformerPipeline =
                source
                        .peek((k,v) -> log.debug("Transformer Pipeline: Picked up Cannonical Trade from inbound topic : K" +k + "\tV:" + v))
                        .filter((k, ct) -> ct != null)
                      .transformValues(() -> new CustomDedupCacheTransformer(ttlMs, maxEntries, hmacSecret))
                        .peek((k,v) -> log.debug("Transformer Pipeline: After Caching Attempt : K" +k + "\tV:" + v))
                      .filter((k, v) -> v != null)  // null means “dropped as duplicate”
                        .peek((k,v) -> log.debug("Transformer Pipeline: After filtering for dropped packets: K" +k + "\tV:" + v))
                        .mapValues(TradeTransformer::transform)
                        .peek((k,v) -> log.debug("Transformer Pipeline: After transformation for dropped packets: K" +k + "\tV:" + v))
                      .selectKey((oldKey, trade) -> CustomDedupCacheTransformer.hmacKey(hmacSecret, trade.getAccount()))
                ;

        transformerPipeline.to (outTopic, Produced.with(Serdes.String(), tradeSerde));
        return transformerPipeline;
    }
} //KafkaStreamTransformer
