package com.rbkmoney.payout.manager.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.rbkmoney.damsel.domain.Cash;
import com.rbkmoney.damsel.domain.Contract;
import com.rbkmoney.damsel.domain.FinalCashFlowPosting;
import com.rbkmoney.damsel.domain.Party;
import com.rbkmoney.damsel.shumpune.Balance;
import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.dao.DaoException;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.geck.serializer.kit.json.JsonHandler;
import com.rbkmoney.geck.serializer.kit.tbase.TBaseProcessor;
import com.rbkmoney.payout.manager.dao.PayoutDao;
import com.rbkmoney.payout.manager.domain.enums.PayoutStatus;
import com.rbkmoney.payout.manager.domain.tables.pojos.Payout;
import com.rbkmoney.payout.manager.exception.InsufficientFundsException;
import com.rbkmoney.payout.manager.exception.InvalidStateException;
import com.rbkmoney.payout.manager.exception.NotFoundException;
import com.rbkmoney.payout.manager.exception.StorageException;
import com.rbkmoney.payout.manager.util.CashFlowType;
import com.rbkmoney.payout.manager.util.DamselUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class PayoutService {

    private final ShumwayService shumwayService;

    private final PartyManagementService partyManagementService;

    private final PayoutDao payoutDao;

    @Transactional(propagation = Propagation.REQUIRED)
    public Payout create(String partyId, String shopId, Cash cash) {
        log.info("Trying to create a payout, partyId='{}', shopId='{}'", partyId, shopId);
        if (cash.getAmount() <= 0) {
            throw new InsufficientFundsException("Available amount must be greater than 0");
        }
        long partyRevision = partyManagementService.getPartyRevision(partyId);
        Party party = partyManagementService.getParty(partyId, partyRevision);
        String payoutToolId = party.getShops().get(shopId).getPayoutToolId();
        Contract contract = getContract(party, payoutToolId, partyId);
        LocalDateTime localDateTime = LocalDateTime.now(ZoneOffset.UTC);
        String createdAt = TypeUtil.temporalToString(localDateTime.toInstant(ZoneOffset.UTC));
        List<FinalCashFlowPosting> cashFlowPostings = partyManagementService.computePayoutCashFlow(
                partyId,
                shopId,
                cash,
                payoutToolId,
                createdAt);
        Map<CashFlowType, Long> cashFlow = DamselUtil.parseCashFlow(cashFlowPostings);
        Long cashFlowAmount = cashFlow.getOrDefault(CashFlowType.PAYOUT_AMOUNT, 0L);
        Long cashFlowPayoutFee = cashFlow.getOrDefault(CashFlowType.PAYOUT_FIXED_FEE, 0L);
        Long cashFlowFee = cashFlow.getOrDefault(CashFlowType.FEE, 0L);
        long amount = cashFlowAmount - cashFlowPayoutFee;
        long fee = cashFlowFee + cashFlowPayoutFee;
        if (amount <= 0) {
            throw new InsufficientFundsException(
                    String.format("Negative amount in payout cash flow, amount='%d', fee='%d'", amount, fee));
        }
        String payoutId = UUID.randomUUID().toString();
        Payout payout = savePayout(
                partyId,
                shopId,
                cash,
                payoutToolId,
                contract,
                localDateTime,
                cashFlowPostings,
                amount,
                fee,
                payoutId);
        Clock clock = shumwayService.hold(payoutId, cashFlowPostings);
        long accountId = party.getShops().get(shopId).getAccount().getSettlement();
        validateBalance(payoutId, clock, accountId);
        log.info("Payout has been created, payoutId='{}'", payoutId);
        return payout;
    }

    public Payout get(String payoutId) {
        log.info("Trying to get a payout, payoutId='{}'", payoutId);
        try {
            return payoutDao.get(payoutId);
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void confirm(String payoutId) {
        log.info("Trying to confirm a payout, payoutId='{}'", payoutId);
        try {
            Payout payout = payoutDao.getForUpdate(payoutId);
            if (payout.getStatus() == PayoutStatus.CONFIRMED) {
                log.info("Payout already confirmed, payoutId='{}'", payoutId);
                return;
            } else if (payout.getStatus() != PayoutStatus.UNPAID) {
                throw new InvalidStateException(
                        String.format("Invalid status for 'confirm' action, payoutId='%s', currentStatus='%s'",
                                payoutId, payout.getStatus())
                );
            }
            payoutDao.changeStatus(payoutId, PayoutStatus.CONFIRMED);
            shumwayService.commit(payoutId);
            log.info("Payout has been confirmed, payoutId='{}'", payoutId);
        } catch (DaoException ex) {
            throw new StorageException(String.format("Failed to confirm a payout, payoutId='%s'", payoutId), ex);
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void cancel(String payoutId) {
        log.info("Trying to cancel a payout, payoutId='{}'", payoutId);
        try {
            Payout payout = payoutDao.getForUpdate(payoutId);
            if (payout.getStatus() == PayoutStatus.CANCELLED) {
                log.info("Payout already cancelled, payoutId='{}'", payoutId);
                return;
            }
            payoutDao.changeStatus(payoutId, PayoutStatus.CANCELLED);
            switch (payout.getStatus()) {
                case UNPAID:
                case PAID:
                    shumwayService.rollback(payoutId);
                    break;
                case CONFIRMED:
                    shumwayService.revert(payoutId);
                    break;
                default:
                    throw new InvalidStateException(String.format("Invalid status for 'cancel' action, " +
                            "payoutId='%s', currentStatus='%s'", payoutId, payout.getStatus()));
            }
            log.info("Payout has been cancelled, payoutId='{}'", payoutId);
        } catch (DaoException ex) {
            throw new StorageException(String.format("Failed to cancel a payout, payoutId='%s'", payoutId), ex);
        }
    }

    private Payout savePayout(
            String partyId,
            String shopId,
            Cash cash,
            String payoutToolId,
            Contract contract,
            LocalDateTime localDateTime,
            List<FinalCashFlowPosting> cashFlowPostings,
            long amount,
            long fee,
            String payoutId) {
        try {
            var payout = new Payout();
            payout.setPayoutId(payoutId);
            payout.setCreatedAt(localDateTime);
            payout.setPartyId(partyId);
            payout.setShopId(shopId);
            payout.setContractId(contract.getId());
            payout.setStatus(com.rbkmoney.payout.manager.domain.enums.PayoutStatus.UNPAID);
            try {
                payout.setCashFlow(new ObjectMapper().writeValueAsString(cashFlowPostings.stream()
                        .map(cashFlowPosting -> {
                            try {
                                return new TBaseProcessor().process(cashFlowPosting, new JsonHandler());
                            } catch (IOException ex) {
                                throw new RuntimeJsonMappingException(ex.getMessage());
                            }
                        })
                        .collect(Collectors.toList())
                ));
            } catch (IOException ex) {
                throw new RuntimeException("Failed to write cash flow", ex);
            }
            payout.setPayoutToolId(payoutToolId);
            payout.setAmount(amount);
            payout.setFee(fee);
            payout.setCurrencyCode(cash.getCurrency().getSymbolicCode());
            payoutDao.save(payout);
            return payout;
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }

    private Contract getContract(Party party, String payoutToolId, String partyId) {
        return party.getContracts().values().stream()
                .filter(contractValue -> contractValue.getPayoutTools().stream()
                        .anyMatch(payoutToolValue -> payoutToolValue.getId().equals(payoutToolId)))
                .findFirst()
                .orElseThrow(() -> new NotFoundException(
                        String.format("Contract for payout tool not found, " +
                                "partyId='%s', payoutToolId='%s'", partyId, payoutToolId)));
    }

    private void validateBalance(String payoutId, Clock clock, long accountId) {
        Balance balance = shumwayService.getBalance(accountId, clock, payoutId);
        if (balance == null || balance.getMinAvailableAmount() < 0) {
            shumwayService.rollback(payoutId);
            throw new InsufficientFundsException(
                    String.format("Invalid available amount in shop account, balance='%s'", balance));
        }
    }
}
