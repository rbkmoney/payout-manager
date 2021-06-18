package com.rbkmoney.payout.manager.util;

import com.rbkmoney.damsel.domain.FinalCashFlowPosting;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DamselUtil {

    public static Map<CashFlowType, Long> parseCashFlow(List<FinalCashFlowPosting> finalCashFlow) {
        return finalCashFlow.stream()
                .collect(Collectors.groupingBy(CashFlowType::getCashFlowType,
                        Collectors.summingLong(cashFlow -> cashFlow.getVolume().getAmount())));
    }
}
