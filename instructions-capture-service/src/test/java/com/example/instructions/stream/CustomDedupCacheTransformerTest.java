package com.example.instructions.stream;

import com.example.instructions.model.CanonicalTrade;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.context.annotation.Profile;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for CustomDedupCacheTransformer
 */
@Profile("kstreams")
class CustomDedupCacheTransformerTest {

    private CustomDedupCacheTransformer transformer;
    private ProcessorContext mockContext;

    @BeforeEach
    void setUp() {
        mockContext = Mockito.mock(org.apache.kafka.streams.processor.ProcessorContext.class);
        // TTL = 200ms, maxEntries = 100, dummy secret
        transformer = new CustomDedupCacheTransformer(20, 10, "test-secret");
        transformer.init(mockContext);
    }

    /**
     * Tests the behavior of the deduplication mechanism by verifying that the first trade is
     * processed and cached, whereas a duplicate trade within the Time-To-Live (TTL) interval
     * is dropped.
     *
     * This test focuses on ensuring that:
     * - A trade instance is successfully processed and allowed to pass when it is first encountered.
     * - The same trade instance is dropped as a duplicate if submitted again within the defined TTL duration.
     *
     * Test Steps:
     * 1. Construct a {@link CanonicalTrade} instance with specific trade attributes.
     * 2. Transform the trade using the `transformer`, simulating its processing and caching.
     * 3. Attempt to process the same trade a second time within the TTL.
     * 4. Verify correct behavior of the caching and deduplication mechanism.
     *
     * Assertions:
     * - The first transformation of the trade should return a non-null result.
     * - The second transformation of the same trade within the TTL should return null,
     *   indicating that it was correctly identified as a duplicate and dropped.
     */
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

    /**
     * Verifies that a new trade instance is allowed after the Time-To-Live (TTL)
     * for a cached entry has expired.
     *
     * This test simulates a scenario where the same trade is processed twice, separated by a delay
     * exceeding the predefined TTL. The first transformation caches the trade, while the second
     * transformation occurs after the TTL has expired, allowing the trade to be accepted again as
     * a new instance.
     *
     * Steps:
     * 1. Build a {@link CanonicalTrade} object with specific attributes.
     * 2. Transform the trade using the `transformer` and cache it.
     * 3. Introduce a delay that exceeds the TTL duration (200ms).
     * 4. Transform the same trade again, ensuring it is treated as a new instance.
     *
     * Assertions:
     * - Both transformations should return non-null results.
     * - The transformed trades should be distinct (not equal), verifying that the TTL expiration
     *   allows for acceptance as a new trade instance.
     *
     * @throws InterruptedException if the thread sleep operation is interrupted.
     */
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

        Thread.sleep(1000);
        transformer.purge(System.currentTimeMillis());

        // purge executes scheduled at 5ms  -
        CanonicalTrade second = transformer.transform("k1", trade);

        assertNotNull(first, "First trade is present but not available in the cache (cache empty)");
        assertNotNull(second, "Duplicate after TTL should be accepted");

        assertNotEquals(first, second, "After TTL expiry, new instance should be accepted as a new entry and that object should be distinct from the original");
    }

    /**
     * Verifies that expired entries in the cache are correctly purged when the purge method is invoked.
     *
     * This test simulates the scenario where a {@link CanonicalTrade} is transformed and cached,
     * followed by manually triggering the `purge` method to simulate scheduled cleanup. The purpose
     * is to ensure that entries exceeding the Time-To-Live (TTL) are properly removed from the cache.
     *
     * Test Steps:
     * 1. Create a {@link CanonicalTrade} object with specific attributes.
     * 2. Transform the trade using the `transformer` to add it to the cache.
     * 3. Confirm that the cache is not empty after insertion.
     * 4. Invoke the `purge` method with a simulated timestamp beyond the TTL limit.
     * 5. Validate that the cache becomes empty, confirming the removal of expired entries.
     *
     * Assertions:
     * - The cache is not empty after adding a trade through the `transform` method.
     * - The cache is empty after manually invoking the `purge` method, ensuring expired entries are purged.
     */
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

    /**
     * Verifies the system's resilience when the cache exceeds its maximum allowed entries.
     *
     * This test ensures that the cache system can handle scenarios where the number of entries
     * significantly surpasses the specified maximum limit (`maxEntries`) without crashing. It also
     * tests the behavior of the `purge` method to validate that it appropriately manages and reduces
     * the cache size when necessary.
     *
     * Test Steps:
     * 1. Create a base {@link CanonicalTrade} instance with specific attributes.
     * 2. Populate the cache by repeatedly creating and transforming trades with unique keys,
     *    exceeding `maxEntries` by a substantial margin.
     * 3. Measure the cache size before invoking the `purge` method.
     * 4. Call the `purge` method with an appropriate timestamp to trigger cache management.
     * 5. Measure the cache size after invoking the `purge` method.
     *
     * Assertions:
     * - The cache size before purging should be greater than or equal to the `maxEntries` limit.
     * - The cache size after purging should be less than or equal to the size before purging,
     *   indicating that the `purge` method successfully reduces or maintains the cache size.
     */
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

    /**
     * Tests the stability and security of the HMAC key generation.
     *
     * This test ensures that the HMAC key generated for the same input is deterministic
     * (i.e., it produces the same output for the same input) and does not contain
     * any plaintext information from the input.
     *
     * Verifications:
     * - The generated HMAC key for the same input should be identical.
     * - The generated HMAC key should not contain the plaintext input to ensure
     *   the confidentiality of the input data.
     */
    @Test
    void hmacKeyShouldBeStableAndNotPlaintext() {
        String key1 = CustomDedupCacheTransformer.hmacKey("secret", "9876543210");
        String key2 = CustomDedupCacheTransformer.hmacKey("secret", "9876543210");
        assertEquals(key1, key2, "HMAC key should be deterministic");
        assertFalse(key1.contains("9876543210"), "HMAC should not leak plaintext input");
    }
}
