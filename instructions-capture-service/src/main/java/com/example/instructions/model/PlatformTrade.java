package com.example.instructions.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PlatformTrade {

    @NotBlank
    private String platform_id;

    @Valid
    @NotNull
    private CanonicalTrade trade;
}
