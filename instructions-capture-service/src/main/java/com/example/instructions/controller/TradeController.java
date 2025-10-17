package com.example.instructions.controller;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.PlatformTrade;
import com.example.instructions.service.TradeService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@Slf4j
@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/trade")
@Tag(name = "Trade", description = "Trade Instructions ingestion and processing")
public class TradeController {

    private final TradeService tradeService;

    @Operation(summary = "Upload Trade Instructions Capture CSV")
    @PostMapping(value = "/instruction/upload/csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public List<CanonicalTrade> uploadCsv(@RequestPart("file") MultipartFile file) {
        log.info("uploadCsv received filename={} size={}", file.getOriginalFilename(), file.getSize());
        log.debug("uploadCsv received file={}", file);
        return tradeService.processCsv(file);
    }

    @Operation(summary = "Upload Trade Instructions Capture JSON (PlatformTrade with CanonicalTrade payload)")
    @PostMapping(value = "/instruction/upload/json", consumes = MediaType.APPLICATION_JSON_VALUE)
    public CanonicalTrade uploadJson(@Valid @RequestBody PlatformTrade payload) {
        log.debug("uploadJson received json", payload);
        return tradeService.processJson(payload);
    }
}