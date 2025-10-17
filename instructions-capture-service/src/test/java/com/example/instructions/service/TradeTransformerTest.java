package com.example.instructions.service;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.util.TradeTransformer;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TradeTransformerTest {

    @Test
    void transform_masksAndNormalizes() {
        CanonicalTrade raw = CanonicalTrade.builder()
                .account("9876543210")
                .security("abc1234")
                .type("Buy")
                .amount(100000)
                .timestamp(OffsetDateTime.parse("2025-08-04T21:15:33Z"))
                .build();

        CanonicalTrade out = TradeTransformer.transform(raw);
        assertEquals("******3210", out.getAccount());
        assertEquals("ABC1234", out.getSecurity());
        assertEquals("B", out.getType());
        assertEquals(100000, out.getAmount());
    }

    @Test
    void transform_handlesNullInput() {
        CanonicalTrade out = TradeTransformer.transform(null);
        assertNull(out, "Transforming null input should return null");
    }

    @Test
    void transform_normalizesFieldsWithMixedCaseAndWhitespace() {
        CanonicalTrade raw = CanonicalTrade.builder()
                .account(" 1234567890 ")
                .security("  xyzABC ")
                .type(" buy ")
                .amount(50000)
                .timestamp(OffsetDateTime.parse("2025-10-16T14:00:00Z"))
                .build();

        CanonicalTrade out = TradeTransformer.transform(raw);
        assertEquals("******7890", out.getAccount());
        assertEquals("XYZABC", out.getSecurity());
        assertEquals("B", out.getType());
        assertEquals(50000, out.getAmount());
    }

    @Test
    void transform_handlesUnsupportedTradeType() {
        CanonicalTrade raw = CanonicalTrade.builder()
                .account("1234567890")
                .security("ABC1234")
                .type("invalidType")
                .amount(75000)
                .timestamp(OffsetDateTime.parse("2025-08-05T18:30:00Z"))
                .build();

        CanonicalTrade out = TradeTransformer.transform(raw);
        assertEquals("******7890", out.getAccount());
        assertEquals("ABC1234", out.getSecurity());
        assertEquals("U", out.getType(), "Unsupported type should default to 'U'");
        assertEquals(75000, out.getAmount());
    }

    @Test
    void transform_handlesNullFieldsGracefully() {
        CanonicalTrade raw = CanonicalTrade.builder()
                .account(null)
                .security(null)
                .type(null)
                .amount(100000)
                .timestamp(OffsetDateTime.parse("2025-09-15T10:45:00Z"))
                .build();

        CanonicalTrade out = TradeTransformer.transform(raw);
        assertNull(out.getAccount(), "Null account should remain null");
        assertNull(out.getSecurity(), "Null security should remain null");
        assertEquals("U", out.getType(), "Null type should default to 'U'");
        assertEquals(100000, out.getAmount());
    }
}
