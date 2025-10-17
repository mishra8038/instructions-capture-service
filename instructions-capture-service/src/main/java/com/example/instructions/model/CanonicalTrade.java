package com.example.instructions.model;

import jakarta.validation.constraints.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.time.OffsetDateTime;

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
