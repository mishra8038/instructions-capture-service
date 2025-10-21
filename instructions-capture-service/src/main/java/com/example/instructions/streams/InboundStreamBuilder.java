package com.example.instructions.streams;

import com.example.instructions.model.CanonicalTrade;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.support.serializer.JsonSerde;

@Profile("kstreams")
public class InboundStreamBuilder {
  private final String inboundTopic;
  private final JsonSerde<CanonicalTrade> serde;

  public InboundStreamBuilder(String inboundTopic) {
    this.inboundTopic = inboundTopic;
    this.serde = new JsonSerde<>(CanonicalTrade.class);
  }

  public KStream<String, CanonicalTrade> build(StreamsBuilder builder) {
    return builder.stream(inboundTopic, Consumed.with(Serdes.String(), serde)).filter((k, v) -> v != null);
  }
} //InboundStreamBuilder
