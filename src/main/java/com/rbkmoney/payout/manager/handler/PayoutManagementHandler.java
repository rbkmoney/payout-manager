package com.rbkmoney.payout.manager.handler;

import com.rbkmoney.damsel.base.InvalidRequest;
import com.rbkmoney.payout.manager.InsufficientFunds;
import com.rbkmoney.payout.manager.Payout;
import com.rbkmoney.payout.manager.PayoutNotFound;
import com.rbkmoney.payout.manager.PayoutParams;
import com.rbkmoney.payout.manager.exception.InsufficientFundsException;
import com.rbkmoney.payout.manager.exception.InvalidStateException;
import com.rbkmoney.payout.manager.exception.NotFoundException;
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
    private final PayoutKafkaProducerService payoutKafkaProducerService;

    @Override
    public Payout createPayout(PayoutParams payoutParams) throws InsufficientFunds, InvalidRequest, TException {
        try {
            var payout = payoutService.create(
                    payoutParams.getShopParams().getPartyId(),
                    payoutParams.getShopParams().getShopId(),
                    payoutParams.getCash());
            Payout thriftPayout = ThriftUtil.toThriftPayout(payout);
            payoutKafkaProducerService.send(thriftPayout);
            return thriftPayout;
        } catch (InsufficientFundsException ex) {
            throw new InsufficientFunds();
        } catch (NotFoundException ex) {
            throw new InvalidRequest(List.of(ex.getMessage()));
        }
    }

    @Override
    public Payout getPayout(String payoutId) throws PayoutNotFound, TException {
        var payout = payoutService.get(payoutId);
        if (payout == null) {
            throw new PayoutNotFound();
        }
        return ThriftUtil.toThriftPayout(payout);
    }

    @Override
    public void confirmPayout(String payoutId) throws InvalidRequest, TException {
        try {
            payoutService.confirm(payoutId);
        } catch (InvalidStateException | NotFoundException ex) {
            throw new InvalidRequest(List.of(ex.getMessage()));
        }
        sendToKafka(payoutId);
    }

    @Override
    public void cancelPayout(String payoutId, String details) throws InvalidRequest, TException {
        try {
            payoutService.cancel(payoutId);
        } catch (InvalidStateException | NotFoundException ex) {
            throw new InvalidRequest(List.of(ex.getMessage()));
        }
        sendToKafka(payoutId);
    }

    private void sendToKafka(String payoutId) {
        var payout = payoutService.get(payoutId);
        Payout thriftPayout = ThriftUtil.toThriftPayout(payout);
        payoutKafkaProducerService.send(thriftPayout);
    }
}
