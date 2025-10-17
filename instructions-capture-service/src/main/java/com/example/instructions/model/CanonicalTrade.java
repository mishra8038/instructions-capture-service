package com.example.instructions.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.OffsetDateTime;
/**
 * Canonical trade model.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class CanonicalTrade {

    @NotBlank
    private String account;

    @NotBlank
    private String security;


    @NotBlank
    @Pattern(regexp = "B|S|C|Buy|Sell|Cancel|.*", message = "Invalid trade type code")
    private String type;

    @Positive
    private long amount;

    @NotNull
    private OffsetDateTime timestamp;
}
