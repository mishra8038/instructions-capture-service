package com.example.instructions.util;

import com.example.instructions.model.CanonicalTrade;

import java.util.Locale;
import java.util.Map;

public final class TradeTransformer {

    private TradeTransformer() {}

    private static final Map<String, String> TYPE_MAP = Map.of(
            "B", "B",
            "BUY", "B",
            "S", "S",
            "SELL", "S",
            "C", "C",
            "CANCEL", "C"
    );


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

    public static String maskAccount(String accountNumber) {
        if (accountNumber == null) return null;
        accountNumber = accountNumber.trim();
        String accountUnmaskedSubstring = accountNumber.substring(accountNumber.length()-4);
        return accountNumber.substring(0, accountNumber.length() - 4).replaceAll(".", "*") +  accountUnmaskedSubstring;
    }

    public static String normalizeSecurity(String sec) {
        return sec == null ? null : sec.toUpperCase(Locale.ROOT).trim();
    }

    public static String normalizeType(String type) {
        if (type == null) return "U";
        String key = type.trim().toUpperCase(Locale.ROOT);
        return TYPE_MAP.getOrDefault(key, key.length() == 1 ? key : "U");
    }
}
