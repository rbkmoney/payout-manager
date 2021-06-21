package com.rbkmoney.payout.manager.handler;

import com.rbkmoney.damsel.base.InvalidRequest;
import com.rbkmoney.payout.manager.InsufficientFunds;
import com.rbkmoney.payout.manager.Payout;
import com.rbkmoney.payout.manager.PayoutNotFound;
import com.rbkmoney.payout.manager.PayoutParams;
import com.rbkmoney.payout.manager.domain.tables.pojos.CashFlowPosting;
import com.rbkmoney.payout.manager.exception.InsufficientFundsException;
import com.rbkmoney.payout.manager.exception.InvalidStateException;
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
            var payout = payoutService.create(
                    payoutParams.getShopParams().getPartyId(),
                    payoutParams.getShopParams().getShopId(),
                    payoutParams.getCash());
            List<CashFlowPosting> cashFlowPostings = cashFlowPostingService.getCashFlowPostings(payout.getPayoutId());
            Payout thriftPayout = ThriftUtil.toThriftPayout(payout, cashFlowPostings);
            payoutKafkaProducerService.send(thriftPayout);
            return thriftPayout;
        } catch (InsufficientFundsException ex) {
            throw new InsufficientFunds();
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
        } catch (InvalidStateException ex) {
            throw new InvalidRequest(List.of(ex.getMessage()));
        }
    }

    @Override
    public void cancelPayout(String payoutId, String details) throws InvalidRequest, TException {
        try {
            payoutService.cancel(payoutId);
            sendToKafka(payoutId);
        } catch (InvalidStateException ex) {
            throw new InvalidRequest(List.of(ex.getMessage()));
        }
    }

    private void sendToKafka(String payoutId) {
        var payout = payoutService.get(payoutId);
        List<CashFlowPosting> cashFlowPostings = cashFlowPostingService.getCashFlowPostings(payoutId);
        Payout thriftPayout = ThriftUtil.toThriftPayout(payout, cashFlowPostings);
        payoutKafkaProducerService.send(thriftPayout);
    }
}
