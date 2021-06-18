package com.rbkmoney.payout.manager.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.rbkmoney.damsel.domain.CurrencyRef;
import com.rbkmoney.damsel.domain.FinalCashFlowPosting;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.geck.serializer.kit.json.JsonProcessor;
import com.rbkmoney.geck.serializer.kit.tbase.TBaseHandler;
import com.rbkmoney.payout.manager.*;
import com.rbkmoney.payout.manager.exception.NotFoundException;
import org.apache.thrift.TBase;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class ThriftUtil {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonProcessor JSON_PROCESSOR = new JsonProcessor();

    public static Payout toThriftPayout(com.rbkmoney.payout.manager.domain.tables.pojos.Payout payout) {
        return new Payout()
                .setId(payout.getPayoutId())
                .setCreatedAt(TypeUtil.temporalToString(payout.getCreatedAt().toInstant(ZoneOffset.UTC)))
                .setPartyId(payout.getPartyId())
                .setShopId(payout.getShopId())
                .setContractId(payout.getContractId())
                .setStatus(toThriftPayoutStatus(payout.getStatus()))
                .setCashFlow(toThriftCashFlow(payout.getCashFlow()))
                .setPayoutToolId(payout.getPayoutToolId())
                .setAmount(payout.getAmount())
                .setFee(payout.getFee())
                .setCurrency(new CurrencyRef(payout.getCurrencyCode()));
    }

    public static PayoutStatus toThriftPayoutStatus(
            com.rbkmoney.payout.manager.domain.enums.PayoutStatus payoutStatus) {
        switch (payoutStatus) {
            case UNPAID:
                return PayoutStatus.unpaid(new PayoutUnpaid());
            case PAID:
                return PayoutStatus.paid(new PayoutPaid());
            case CONFIRMED:
                return PayoutStatus.confirmed(new PayoutConfirmed());
            case CANCELLED:
                return PayoutStatus.cancelled(new PayoutCancelled());
            default:
                throw new NotFoundException(String.format("Payout status not found, status = %s", payoutStatus));
        }
    }

    public static List<FinalCashFlowPosting> toThriftCashFlow(String cashFlow) {
        List<FinalCashFlowPosting> finalCashFlowPostings = new ArrayList<>();
        try {
            for (JsonNode jsonNode : OBJECT_MAPPER.readTree(cashFlow)) {
                FinalCashFlowPosting finalCashFlowPosting = jsonToTBase(jsonNode, FinalCashFlowPosting.class);
                finalCashFlowPostings.add(finalCashFlowPosting);
            }
        } catch (IOException ex) {
            throw new RuntimeJsonMappingException(ex.getMessage());
        }
        return finalCashFlowPostings;
    }

    public static <T extends TBase> T jsonToTBase(JsonNode jsonNode, Class<T> type) throws IOException {
        return JSON_PROCESSOR.process(jsonNode, new TBaseHandler<>(type));
    }
}
