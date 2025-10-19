package com.example.instructions.stream;

import com.example.instructions.model.CanonicalTrade;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for CustomDedupCacheTransformer
 */
class CustomDedupCacheTransformerTest {

    private CustomDedupCacheTransformer transformer;
    private ProcessorContext mockContext;

    @BeforeEach
    void setUp() {
        mockContext = Mockito.mock(org.apache.kafka.streams.processor.ProcessorContext.class);
        // TTL = 200ms, maxEntries = 100, dummy secret
        transformer = new CustomDedupCacheTransformer(200, 100, "test-secret");
        transformer.init(mockContext);
    }

    @Test
    void shouldAllowFirstTradeAndDropDuplicateWithinTtl() {
        CanonicalTrade trade = CanonicalTrade.builder()
                .account("9876543210")
                .security("ABC123")
                .type("B")
                .amount(100000)
                .timestamp(OffsetDateTime.parse("2025-08-04T21:15:33Z"))
                .build();

        CanonicalTrade first = transformer.transform("key1", trade);
        CanonicalTrade second = transformer.transform("key1", trade);

        assertNotNull(first, "First trade should pass through (cache empty)");
        assertNull(second, "Duplicate within TTL should be dropped");
    }

    @Test
    void shouldAllowTradeAfterTtlExpires() throws InterruptedException {
        CanonicalTrade trade = CanonicalTrade.builder()
                .account("9876543210")
                .security("XYZ999")
                .type("B")
                .amount(100000)
                .timestamp(OffsetDateTime.parse("2025-08-04T21:15:33Z"))
                .build();

        CanonicalTrade first = transformer.transform("k1", trade);
        Thread.sleep(250); // TTL = 200ms, so expire the cache entry
        CanonicalTrade second = transformer.transform("k1", trade);

        assertNotNull(first);
        assertNotNull(second);
        assertNotEquals(first, second, "After TTL expiry, new instance should be accepted");
    }

    @Test
    void shouldPurgeExpiredEntries() {
        CanonicalTrade trade = CanonicalTrade.builder()
                .account("9876543210")
                .security("ZED")
                .type("B")
                .amount(100000)
                .timestamp(OffsetDateTime.parse("2025-08-04T21:15:33Z"))
                .build();

        transformer.transform("a", trade);
        assertFalse(transformer.getCache().isEmpty(), "Cache should have entries");
        // call purge manually to simulate scheduled punctuator
        transformer.purge(System.currentTimeMillis() + 1000);
        assertTrue(transformer.getCache().isEmpty(), "Expired entries should be purged");
    }

    @Test
    void shouldNotCrashWhenCacheOverMaxEntries() {
        CanonicalTrade base = CanonicalTrade.builder()
                .account("9876543210")
                .security("S")
                .type("B")
                .amount(1)
                .timestamp(OffsetDateTime.parse("2025-08-04T21:15:33Z"))
                .build();

        // fill well past maxEntries
        for (int i = 0; i < 200; i++) {
            CanonicalTrade t = base.builder().security("S" + i).build();
            transformer.transform("key-" + i, t);
        }

        int before = transformer.getCache().size();
        transformer.purge(System.currentTimeMillis() + 1000);
        int after = transformer.getCache().size();

        assertTrue(before >= 100, "Cache should exceed maxEntries");
        assertTrue(after <= before, "Purge should reduce or maintain size");
    }

    @Test
    void hmacKeyShouldBeStableAndNotPlaintext() {
        String key1 = CustomDedupCacheTransformer.hmacKey("secret", "9876543210");
        String key2 = CustomDedupCacheTransformer.hmacKey("secret", "9876543210");
        assertEquals(key1, key2, "HMAC key should be deterministic");
        assertFalse(key1.contains("9876543210"), "HMAC should not leak plaintext input");
    }
}
