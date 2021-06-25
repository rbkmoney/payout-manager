package com.rbkmoney.payout.manager.handler;

import com.rbkmoney.damsel.base.InvalidRequest;
import com.rbkmoney.payout.manager.*;
import com.rbkmoney.payout.manager.domain.tables.pojos.CashFlowPosting;
import com.rbkmoney.payout.manager.exception.InsufficientFundsException;
import com.rbkmoney.payout.manager.exception.InvalidRequestException;
import com.rbkmoney.payout.manager.exception.NotFoundException;
import com.rbkmoney.payout.manager.service.CashFlowPostingService;
import com.rbkmoney.payout.manager.service.PayoutKafkaProducerService;
import com.rbkmoney.payout.manager.service.PayoutService;
import com.rbkmoney.payout.manager.util.ThriftUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class PayoutManagementHandler implements com.rbkmoney.payout.manager.PayoutManagementSrv.Iface {

    private final PayoutService payoutService;
    private final CashFlowPostingService cashFlowPostingService;
    private final PayoutKafkaProducerService payoutKafkaProducerService;

    @Override
    public Payout createPayout(PayoutParams payoutParams) throws InsufficientFunds, InvalidRequest, TException {
        try {
            String payoutId = payoutService.create(
                    payoutParams.getShopParams().getPartyId(),
                    payoutParams.getShopParams().getShopId(),
                    payoutParams.getCash());
            sendToKafka(payoutId);
            return getPayout(payoutId);
        } catch (InsufficientFundsException ex) {
            throw new InsufficientFunds();
        } catch (InvalidRequestException ex) {
            throw new InvalidRequest(
                    Optional.ofNullable(ex.getMessage()).map(List::of).orElse(List.of()));
        }
    }

    @Override
    public Payout getPayout(String payoutId) throws PayoutNotFound, TException {
        try {
            var payout = payoutService.get(payoutId);
            List<CashFlowPosting> cashFlowPostings = cashFlowPostingService.getCashFlowPostings(payout.getPayoutId());
            return ThriftUtil.toThriftPayout(payout, cashFlowPostings);
        } catch (NotFoundException ex) {
            throw new PayoutNotFound();
        }
    }

    @Override
    public void confirmPayout(String payoutId) throws InvalidRequest, TException {
        try {
            payoutService.confirm(payoutId);
            sendToKafka(payoutId);
        } catch (InvalidRequestException ex) {
            throw new InvalidRequest(
                    Optional.ofNullable(ex.getMessage()).map(List::of).orElse(List.of()));
        }
    }

    @Override
    public void cancelPayout(String payoutId, String details) throws InvalidRequest, TException {
        try {
            payoutService.cancel(payoutId, details);
            sendToKafka(payoutId);
        } catch (InvalidRequestException ex) {
            throw new InvalidRequest(
                    Optional.ofNullable(ex.getMessage()).map(List::of).orElse(List.of()));
        }
    }

    private void sendToKafka(String payoutId) {
        var payout = payoutService.get(payoutId);
        List<CashFlowPosting> cashFlowPostings = cashFlowPostingService.getCashFlowPostings(payoutId);
        Event event = ThriftUtil.createEvent(payout, cashFlowPostings);
        payoutKafkaProducerService.send(event);
    }
}
