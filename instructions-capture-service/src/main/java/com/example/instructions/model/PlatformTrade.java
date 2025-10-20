package com.example.instructions.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
/**
 * Platform trade model which serves as a wrapper for the canonical trade model.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PlatformTrade {
    @NotBlank private String platform_id;
    @Valid @NotNull private CanonicalTrade trade;
}//PlatformTrade