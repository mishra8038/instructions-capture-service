package com.example.instructions.controller;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.PlatformTrade;
import com.example.instructions.service.TradeService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@Slf4j
@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/trade")
@Tag(name = "Trade", description = "Trade instructions ingestion and processing")
public class TradeController {

    private final TradeService tradeService;

    /**
     * Method exposes a rest endpoint to upload a csv or json file containing trade instructions.
     * @param file - the file to be uploaded
     * @return - ResponseEntity with a is of well-formed Canonica Trades.
     */
    @Operation(summary = "Upload Trade Instructions Capture CSV")
    @PostMapping(value = "/instructions", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<List<CanonicalTrade>> uploadCsv(@RequestParam ("file") MultipartFile file){
        log.info("uploadCsv received filename={} size={}", file.getOriginalFilename(), file.getSize());
        log.debug("uploadCsv received file={}", file);
        if (file.isEmpty()) throw new RuntimeException("Failed to upload file: empty");
        if (file.getContentType() == null) throw new RuntimeException("Failed to upload file: no content type");
        if (file.getContentType().equals("text/csv")) {
            log.info("Received CSV File ={} size={}", file.getOriginalFilename(), file.getSize());
            return ResponseEntity.status(HttpStatus.OK).body(tradeService.processBulkUploadTradeInstrutionsCsv(file));
        }
        if (file.getContentType().equals("application/json")) {
            log.info("Received JSON File ={} size={}", file.getOriginalFilename(), file.getSize());
            return ResponseEntity.status(HttpStatus.OK).body(tradeService.processBulkUploadTradeInstrutionsJSON(file));
        }
        //if processing of the file by the above methods fails return an empty response with HTTP Processing Code
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(null);
    }


    /**
     * Method exposes a rest endpoint to process a json payload containing a trade instruction.
     * @param payload - the trade instruction payload
     * @return well-formed CanonicalTrade object
     */
    @Operation(summary = "Upload Trade Instructions Capture JSON (PlatformTrade with CanonicalTrade payload)")
    @PostMapping (value = "/instructions", consumes = MediaType.APPLICATION_JSON_VALUE)
    public CanonicalTrade processPlatformTrade(@Valid @RequestBody PlatformTrade payload) {
        log.info ("Processing individual Trade Instruction.", payload);
        return tradeService.processTradeInstruction (payload);
    }

}