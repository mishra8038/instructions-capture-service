package com.example.instructions.streams;

import com.example.instructions.model.CanonicalTrade;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@Profile("kstreams")
@EnableKafkaStreams
public class TransformTopology {

  @Value("${app.kafka.topics.inbound:instructions.inbound}") private String inbound;
  @Value("${app.kafka.topics.outbound:instructions.outbound}") private String outbound;

  @Bean public KStream<String, CanonicalTrade> tradeTransformPipeline(StreamsBuilder builder) {
    InboundStreamBuilder in = new InboundStreamBuilder(inbound);
    KStream<String, CanonicalTrade> source = in.build(builder);
    KStream<String, CanonicalTrade> transformed = source.transformValues(TradeMaskingTransformer.supplier());
    OutboundStreamSink out = new OutboundStreamSink(outbound);
    out.write(transformed);

    return transformed;
  } //tradeTransformPipeline
} //TradeMaskingTransformer
