package com.example.instructions.streams;

import com.example.instructions.model.CanonicalTrade;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.support.serializer.JsonSerde;

@Profile("kstreams")
public class OutboundStreamSink {
  private final String outboundTopic;
  private final JsonSerde<CanonicalTrade> serde;

  public OutboundStreamSink(String outboundTopic) {
    this.outboundTopic = outboundTopic;
    this.serde = new JsonSerde<>(CanonicalTrade.class);
  }

  public void write(KStream<String, CanonicalTrade> stream) {
    stream.to(outboundTopic, Produced.with(Serdes.String(), serde));
  }
} //OutboundStreamSink
