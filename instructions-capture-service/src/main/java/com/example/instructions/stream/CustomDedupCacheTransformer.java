package com.example.instructions.stream;

import com.example.instructions.model.CanonicalTrade;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-instance, lock-free dedup + cache:
 * - Computes a privacy-safe dedupe key from trade fields (HMAC).
 * - Drops duplicates seen in the hot window (TTL).
 * - Periodically purges expired entries to cap memory.
 */
@Getter
@Slf4j
public class CustomDedupCacheTransformer implements ValueTransformerWithKey<String, CanonicalTrade, CanonicalTrade> {

    private final long ttlMs;
    private final int maxEntries;
    private final String hmacSecret;

    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();

    private ProcessorContext context;

    public CustomDedupCacheTransformer(long ttlMs, int maxEntries, String hmacSecret) {
        this.ttlMs = ttlMs;
        this.maxEntries = Math.max(10_000, maxEntries);
        this.hmacSecret = Objects.requireNonNull(hmacSecret);
    }

    @Override public void init(org.apache.kafka.streams.processor.ProcessorContext context) {
        this.context = context;
        // Periodic purge to remove expired keys and perform soft size control.
        context.schedule(Duration.ofSeconds(Math.max(5, ttlMs / 1000 / 6)), PunctuationType.WALL_CLOCK_TIME, this::purge);
    }


    /**
     * Transforms the given {@link CanonicalTrade} object while applying deduplication logic based on a time-to-live (TTL) window.
     * If the trade is detected as a duplicate within the configured TTL, the method will return {@code null}.
     * Otherwise, it updates the deduplication cache and returns the original trade for further processing.
     *
     * @param readKey the key associated with the incoming trade, used for Kafka streams processing
     * @param value the {@link CanonicalTrade} object to be transformed
     * @return the original {@link CanonicalTrade} object if it is not a duplicate, or {@code null} if it is within the TTL window
     */
    @Override
    public CanonicalTrade transform(String readKey, CanonicalTrade value) {
        // Build a dedupe key from business fields; include timestamp
        String dedupeKey = dedupeId(hmacSecret, value);
        long now = System.currentTimeMillis();
        CacheEntry prev = cache.get(dedupeKey);
        if (prev != null && (now - prev.getSeenAt()) < ttlMs) {
            // Duplicate inside TTL window — drop by returning null
            if (log.isDebugEnabled()) log.debug("Dedup hit key={} ageMs={}", dedupeKey, now - prev.getSeenAt());
            prev.touch(now);
            return null;
        }

        // New (or expired) — record presence for future duplicates.
        cache.put(dedupeKey, new CacheEntry(now));
        return value; // downstream TradeTransformer will do the mask/normalize
    }

    @Override
    public void close() { cache.clear(); }

    /**
     * Purge expired entries and sof cap stale eviction the size (O(n) but executed infrequently).
     */
    void purge(long now) {
        long expiredBefore = now - ttlMs;
        int removed = 0;

        // TTL purge
        Iterator<Map.Entry<String, CacheEntry>> it = cache.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, CacheEntry> e = it.next();
            if (e.getValue().getSeenAt() < expiredBefore) {
                it.remove();
                removed++;
            }
        }

        // If the cache is still too big, evict the stalest ~1% to make room (cheap and fast)
        int size = cache.size();
        if (size > maxEntries) {
            //evict the stalest 1%
            int evictTarget = Math.max(1, size / 100);
            // Opportunistic scan
            it = cache.entrySet().iterator();
            int evicted = 0;
            long cutoff = now - (ttlMs / 2);
            while (it.hasNext() && evicted < evictTarget) { if (it.next().getValue().getSeenAt() < cutoff) { it.remove(); evicted++; } }
            removed += evicted;
        }
        if (removed > 0 && log.isInfoEnabled()) { log.info("cache_purge removed={} size_now={}", removed, "Cache size now is :" + cache.size()); }
    } //purge

    /**
     * Builds a privacy-safe composite dedupe id using HMAC over key business fields.
     */
    static String dedupeId(String secret, CanonicalTrade t) {
        String base = (t.getAccount() == null ? "" : t.getAccount()) + "|" +
                (t.getSecurity() == null ? "" : t.getSecurity()) + "|" +
                (t.getType() == null ? "" : t.getType()) + "|" +
                t.getAmount() + "|" +
                // round timestamp to minute to reduce key explosion for replays
                (t.getTimestamp() == null ? "" : t.getTimestamp().toInstant().getEpochSecond() / 60);
        return hmacKey(secret, base);
    } //dedupeId

    /**
     * Public so kafka streams topology can also key the record by the same privacy-safe HMAC.
     */
    public static String hmacKey(String secret, String input) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] out = mac.doFinal((input == null ? "" : input).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(out.length * 2);
            for (byte b : out) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            return "0";
        }
    } //hmacKey computation

    @Getter
    @AllArgsConstructor
    private static final class CacheEntry {
        private volatile long seenAt;
        void touch(long ts) {
            this.seenAt = ts;
        }
    } // CacheEntry

    // Supplier helper (if you prefer transformValues(CustomDedupCacheTransformer::supplier))
    public static ValueTransformerWithKeySupplier<String, CanonicalTrade, CanonicalTrade> supplier(long ttlMs, int maxEntries, String hmacSecret) {
        return () -> new CustomDedupCacheTransformer(ttlMs, maxEntries, hmacSecret);
    }
}
