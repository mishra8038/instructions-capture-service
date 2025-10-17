package com.example.instructions.service;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.PlatformTrade;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class TradeServiceTest {

    @Test
    void processJson_publishesAndStores() {
        KafkaPublisher publisher = Mockito.mock(KafkaPublisher.class);
        TradeService svc = new TradeService(publisher);

        PlatformTrade pt = PlatformTrade.builder()
                .platform_id("ACCT123")
                .trade(CanonicalTrade.builder()
                        .account("9876543210")
                        .security("abc1234")
                        .type("B")
                        .amount(100000)
                        .timestamp(OffsetDateTime.parse("2025-08-04T21:15:33Z"))
                        .build())
                .build();

        CanonicalTrade out = svc.processTradeInstruction(pt);

        verify(publisher, times(1)).publishCanonical(any());
        assertEquals("ABC1234", out.getSecurity());
        assertEquals("B", out.getType());
        assertEquals("******3210", out.getAccount());
    }
}
