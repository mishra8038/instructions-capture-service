package com.example.instructions.util;

import com.example.instructions.model.CanonicalTrade;

import java.util.Locale;
import java.util.Map;


/**
 * Utility class responsible for transforming and normalizing trade data into its canonical form.
 * The transformation includes masking sensitive information, normalizing field formats,
 * and mapping trade types to standardized codes.
 *
 * This class is final and cannot be instantiated.
 */
public final class TradeTransformer {

    private TradeTransformer() {}

    /**
     * A mapping of trade type keys to their respective standardized trade type codes.
     * This map normalizes various representations of trade actions into consistent canonical codes.
     *
     * Keys:
     * - "B" and "BUY": Represent buy actions, standardized to "B".
     * - "S" and "SELL": Represent sell actions, standardized to "S".
     * - "C" and "CANCEL": Represent cancel actions, standardized to "C".
     *
     * Values:
     * - Each key corresponds to either itself or its normalized version as the value.
     *
     * This map is used in the trade transformation process to ensure uniformity in trade type representations.
     */
    private static final Map<String, String> TYPE_MAP = Map.of(
            "B", "B",
            "BUY", "B",
            "S", "S",
            "SELL", "S",
            "C", "C",
            "CANCEL", "C"
    );


    /**
     * Transforms a given {@link CanonicalTrade} object into a normalized and standardized canonical form.
     * The transformation includes masking the account number, normalizing the security field, and converting
     * the trade type into a predefined canonical format.
     *
     * @param in the input {@link CanonicalTrade} object to be transformed; may be null
     * @return a new {@link CanonicalTrade} object containing the transformed and normalized fields,
     *         or null if the input is null
     */
    public static CanonicalTrade transform(CanonicalTrade in) {
        if (in == null) return null;
        CanonicalTrade out = CanonicalTrade.builder()
                .account(maskAccount(in.getAccount()))
                .security(normalizeSecurity(in.getSecurity()))
                .type(normalizeType(in.getType()))
                .amount(in.getAmount())
                .timestamp(in.getTimestamp())
                .build();
        return out;
    }

    /**
     * Masks an account number by replacing all but the last four characters with asterisks.
     * If the input is null, the method returns null.
     *
     * @param accountNumber the account number to be masked; may be null
     * @return the masked account number with all characters except the last four replaced by asterisks,
     *         or null if the input is null
     */
    public static String maskAccount(String accountNumber) {
        if (accountNumber == null) return null;
        accountNumber = accountNumber.trim();
        String accountUnmaskedSubstring = accountNumber.substring(accountNumber.length()-4);
        return accountNumber.substring(0, accountNumber.length() - 4).replaceAll(".", "*") +  accountUnmaskedSubstring;
    }

    /**
     * Normalizes the given security string by converting it to uppercase using the root locale
     * and trimming any leading or trailing whitespace.
     * If the input is null, the method returns null.
     *
     * @param sec the security string to be normalized; may be null
     * @return the normalized security string, or null if the input is null
     */
    public static String normalizeSecurity(String sec) {
        return sec == null ? null : sec.toUpperCase(Locale.ROOT).trim();
    }

    /**
     * Normalizes a given trade type string to a canonical format.
     * If the input type is null, it defaults to "U". The method converts
     * the type to uppercase, trims any leading or trailing whitespace, and
     * maps it to a predefined set of normalized types using a type map. If no
     * mapping is found, it returns the input string in uppercase if its length is 1,
     * or defaults to "U".
     *
     * @param type the trade type string to be normalized; may be null
     * @return the normalized trade type string, or "U" if the input is null or cannot
     *         be matched to a predefined type
     */
    public static String normalizeType(String type) {
        if (type == null) return "U";
        String key = type.trim().toUpperCase(Locale.ROOT);
        return TYPE_MAP.getOrDefault(key, key.length() == 1 ? key : "U");
    }
}
