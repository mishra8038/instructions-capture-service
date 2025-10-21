package com.example.instructions.streams;

import com.example.instructions.model.CanonicalTrade;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.context.annotation.Profile;

import java.util.Locale;

@Profile("kstreams")
public class TradeMaskingTransformer implements ValueTransformer<CanonicalTrade, CanonicalTrade> {

  private ProcessorContext context;

  @Override public void init(ProcessorContext context) { this.context = context; }

  @Override public CanonicalTrade transform(CanonicalTrade in) {
    if (in == null) return null;
    return CanonicalTrade.builder()
        .account(mask(in.getAccount()))
        .security(upper(in.getSecurity()))
        .type(in.getType())
        .amount(in.getAmount())
        .timestamp(in.getTimestamp())
        .build();
  }

  @Override
  public void close() {}

  private static String upper(String s) {
    return s == null ? null : s.trim().toUpperCase(Locale.ROOT);
  }

  private static String mask(String acct) {
    if (acct == null) return null;
    String digits = acct.replaceAll("\\D", "");
    if (digits.length() <= 4) return digits;
    String last4 = digits.substring(digits.length() - 4);
    return "*".repeat(digits.length() - 4) + last4;
  }

  public static ValueTransformerSupplier<CanonicalTrade, CanonicalTrade> supplier() {
    return TradeMaskingTransformer::new;
  }
} //TradeMaskingTransformer
