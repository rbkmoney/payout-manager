package com.rbkmoney.payout.manager.service;

import com.rbkmoney.damsel.base.InvalidRequest;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.shumpune.*;
import com.rbkmoney.payout.manager.dao.CashFlowPostingDao;
import com.rbkmoney.payout.manager.domain.enums.AccountType;
import com.rbkmoney.payout.manager.domain.tables.pojos.CashFlowPosting;
import com.rbkmoney.payout.manager.exception.AccounterException;
import com.rbkmoney.payout.manager.exception.NotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShumwayService {

    private final AccounterSrv.Iface shumwayClient;
    private final RetryTemplate retryTemplate;
    private final CashFlowPostingDao cashFlowPostingDao;

    public Clock hold(String payoutId, List<FinalCashFlowPosting> finalCashFlowPostings) {
        log.debug("Trying to hold payout postings, payoutId='{}', finalCashFlowPostings='{}'",
                payoutId, finalCashFlowPostings);
        String postingPlanId = toPlanId(payoutId);
        long batchId = 1L;
        List<CashFlowPosting> cashFlowPostings = toCashFlowPostings(
                payoutId,
                postingPlanId,
                batchId,
                finalCashFlowPostings);
        cashFlowPostingDao.save(cashFlowPostings);
        try {
            PostingBatch postingBatch = toPostingBatches(cashFlowPostings).get(0);
            Clock clock = hold(postingPlanId, postingBatch);
            log.info("Payout has been held, payoutId='{}', postingBatch='{}', clock='{}'",
                    payoutId, postingBatch, clock);
            return clock;
        } catch (Exception ex) {
            throw new AccounterException(String.format("Failed to hold payout, payoutId='%s'", payoutId), ex);
        }
    }

    private Clock hold(String postingPlanId, PostingBatch postingBatch) throws TException {
        try {
            log.debug("Start hold operation, postingPlanId='{}', postingBatch='{}'", postingPlanId, postingBatch);
            return retryTemplate.execute(
                    context -> shumwayClient.hold(new PostingPlanChange(postingPlanId, postingBatch)));
        } finally {
            log.debug("End hold operation, postingPlanId='{}', postingBatch='{}'", postingPlanId, postingBatch);
        }
    }

    public void commit(String payoutId) {
        log.debug("Trying to commit payout postings, payoutId='{}'", payoutId);
        List<CashFlowPosting> cashFlowPostings = getCashFlowPostings(payoutId);
        try {
            String postingPlanId = toPlanId(payoutId);
            List<PostingBatch> postingBatches = toPostingBatches(cashFlowPostings);
            commit(postingPlanId, postingBatches);
            log.info("Payout has been committed, payoutId='{}', postingBatches='{}'", payoutId, postingBatches);
        } catch (Exception ex) {
            throw new AccounterException(String.format("Failed to commit payout, payoutId='%s'", payoutId), ex);
        }
    }

    public void commit(String postingPlanId, List<PostingBatch> postingBatches) throws TException {
        try {
            log.debug("Start commit operation, postingPlanId='{}', postingBatches='{}'",
                    postingPlanId, postingBatches);
            retryTemplate.execute(
                    context -> shumwayClient.commitPlan(new PostingPlan(postingPlanId, postingBatches)));
        } finally {
            log.debug("End commit operation, postingPlanId='{}', postingBatches='{}'",
                    postingPlanId, postingBatches);
        }
    }

    public void rollback(String payoutId) {
        log.debug("Trying to rollback payout postings, payoutId='{}'", payoutId);
        List<CashFlowPosting> cashFlowPostings = getCashFlowPostings(payoutId);
        try {
            String postingPlanId = toPlanId(payoutId);
            List<PostingBatch> postingBatches = toPostingBatches(cashFlowPostings);
            rollback(postingPlanId, postingBatches);
            log.info("Payout has been rolled back, payoutId='{}', postingBatches='{}'", payoutId, postingBatches);
        } catch (Exception ex) {
            throw new AccounterException(String.format("Failed to rollback payout, payoutId='%s'", payoutId), ex);
        }
    }

    public void rollback(String postingPlanId, List<PostingBatch> postingBatches) throws TException {
        try {
            log.debug("Start rollback operation, postingPlanId='{}', postingBatches='{}'",
                    postingPlanId, postingBatches);
            retryTemplate.execute(
                    context -> shumwayClient.rollbackPlan(new PostingPlan(postingPlanId, postingBatches)));
        } finally {
            log.debug("End rollback operation, postingPlanId='{}', postingBatches='{}'",
                    postingPlanId, postingBatches);
        }
    }

    public void revert(String payoutId) {
        log.debug("Trying to revert payout, payoutId='{}'", payoutId);
        List<CashFlowPosting> cashFlowPostings = getCashFlowPostings(payoutId);
        try {
            String revertPlanId = toRevertPlanId(payoutId);
            PostingBatch revertPostingBatch = revertPostingBatch(
                    toPostingBatches(cashFlowPostings),
                    posting -> {
                        Posting revertPosting = new Posting(posting);
                        revertPosting.setFromId(posting.getToId());
                        revertPosting.setToId(posting.getFromId());
                        revertPosting.setDescription("Revert payout: " + payoutId);
                        return revertPosting;
                    });
            revert(revertPlanId, revertPostingBatch);
            log.info("Payout has been reverted, " +
                            "payoutId='{}', revertPostingBatch='{}'",
                    payoutId, revertPostingBatch);
        } catch (Exception ex) {
            throw new AccounterException(String.format("Failed to revert payout, payoutId='%s'", payoutId), ex);
        }
    }

    private void revert(String revertPlanId, PostingBatch revertPostingBatch) throws Exception {
        try {
            log.debug("Start revert operation, revertPlanId='{}', revertPostingBatch='{}'",
                    revertPlanId, revertPostingBatch);
            hold(revertPlanId, revertPostingBatch);
            commit(revertPlanId, List.of(revertPostingBatch));
        } catch (Exception ex) {
            processRollbackRevertWhenError(revertPlanId, List.of(revertPostingBatch), ex);
        } finally {
            log.debug("End revert operation, revertPlanId='{}', revertPostingBatch='{}'",
                    revertPlanId, revertPostingBatch);
        }
    }

    private PostingBatch revertPostingBatch(
            List<PostingBatch> postingBatches,
            Function<Posting, Posting> howToRevert) {
        List<Posting> revertPosting = postingBatches.stream()
                .sorted(Comparator.comparing(PostingBatch::getId))
                .flatMap(postingBatch -> postingBatch.getPostings().stream())
                .map(howToRevert)
                .collect(Collectors.toList());
        return new PostingBatch(1L, revertPosting);
    }

    private void processRollbackRevertWhenError(
            String revertPlanId,
            List<PostingBatch> revertPostingBatches,
            Exception parent) throws Exception {
        try {
            rollback(revertPlanId, revertPostingBatches);
        } catch (Exception ex) {
            if (!(ex instanceof InvalidRequest)) {
                log.error("Inconsistent state of postings in shumway, revertPlanId='{}', revertPostingBatches='{}'",
                        revertPlanId, revertPostingBatches, ex);
            }
            var rollbackEx = new RuntimeException(
                    String.format("Failed to rollback postings from revert action, " +
                                    "revertPlanId='%s', revertPostingBatches='%s'",
                            revertPlanId, revertPostingBatches),
                    ex);
            rollbackEx.addSuppressed(parent);
            throw rollbackEx;
        }
        throw parent;
    }

    public Balance getBalance(Long accountId, Clock clock, String payoutId) {
        String clockLog = clock.isSetLatest() ? "Latest" : Arrays.toString(clock.getVector().getState());
        try {
            return getBalance(accountId, clock, payoutId, clockLog);
        } catch (Exception e) {
            throw new AccounterException(
                    String.format("Failed to getBalance, " +
                                    "payoutId='%s', accountId='%s', clock='%s'",
                            payoutId, accountId, clockLog),
                    e);
        }
    }

    private Balance getBalance(Long accountId, Clock clock, String payoutId, String clockLog) throws TException {
        try {
            log.debug("Start getBalance operation, payoutId='{}', accountId='{}', clock='{}'",
                    payoutId, accountId, clockLog);
            return retryTemplate.execute(
                    context -> shumwayClient.getBalanceByID(accountId, clock));
        } finally {
            log.debug("End getBalance operation, payoutId='{}', accountId='{}', clock='{}'",
                    payoutId, accountId, clockLog);
        }
    }

    private List<CashFlowPosting> getCashFlowPostings(String payoutId) {
        List<CashFlowPosting> cashFlowPostings = cashFlowPostingDao.getByPayoutId(payoutId);
        if (cashFlowPostings.isEmpty()) {
            throw new NotFoundException(
                    String.format("Cash flow posting not found, payoutId='%s'", payoutId));
        }
        return cashFlowPostings;
    }

    private List<CashFlowPosting> toCashFlowPostings(
            String payoutId,
            String planId,
            Long batchId,
            List<FinalCashFlowPosting> cashFlowPostings
    ) {
        return cashFlowPostings.stream()
                .map(finalCashFlowPosting -> {
                    CashFlowPosting cashFlowPosting = new CashFlowPosting();
                    FinalCashFlowAccount source = finalCashFlowPosting.getSource();
                    cashFlowPosting.setFromAccountId(source.getAccountId());
                    cashFlowPosting.setFromAccountType(toAccountType(source.getAccountType()));
                    FinalCashFlowAccount destination = finalCashFlowPosting.getDestination();
                    cashFlowPosting.setToAccountId(destination.getAccountId());
                    cashFlowPosting.setToAccountType(toAccountType(destination.getAccountType()));
                    cashFlowPosting.setAmount(finalCashFlowPosting.getVolume().getAmount());
                    cashFlowPosting.setCurrencyCode(finalCashFlowPosting.getVolume().getCurrency().getSymbolicCode());
                    cashFlowPosting.setDescription(buildCashFlowDescription(payoutId, finalCashFlowPosting));
                    cashFlowPosting.setPayoutId(payoutId);
                    cashFlowPosting.setPlanId(planId);
                    cashFlowPosting.setBatchId(batchId);
                    return cashFlowPosting;
                })
                .collect(Collectors.toList());
    }

    private List<PostingBatch> toPostingBatches(List<CashFlowPosting> postings) {
        return postings.stream()
                .collect(Collectors.groupingBy(CashFlowPosting::getBatchId, Collectors.toList()))
                .entrySet().stream()
                .map(entry -> toPostingBatch(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private PostingBatch toPostingBatch(long batchId, List<CashFlowPosting> postings) {
        return new PostingBatch(
                batchId,
                postings.stream()
                        .map(this::toPosting)
                        .collect(Collectors.toList()));
    }

    private Posting toPosting(CashFlowPosting cashFlowPosting) {
        Posting posting = new Posting();
        posting.setFromId(cashFlowPosting.getFromAccountId());
        posting.setToId(cashFlowPosting.getToAccountId());
        posting.setAmount(cashFlowPosting.getAmount());
        posting.setCurrencySymCode(cashFlowPosting.getCurrencyCode());
        posting.setDescription(cashFlowPosting.getDescription());
        return posting;
    }

    private AccountType toAccountType(CashFlowAccount cashFlowAccount) {
        CashFlowAccount._Fields cashFlowAccountType = cashFlowAccount.getSetField();
        switch (cashFlowAccountType) {
            case SYSTEM:
                if (cashFlowAccount.getSystem() == SystemCashFlowAccount.settlement) {
                    return AccountType.SYSTEM_SETTLEMENT;
                }
                throw new IllegalArgumentException();
            case EXTERNAL:
                switch (cashFlowAccount.getExternal()) {
                    case income:
                        return AccountType.EXTERNAL_INCOME;
                    case outcome:
                        return AccountType.EXTERNAL_OUTCOME;
                    default:
                        throw new IllegalArgumentException();
                }
            case MERCHANT:
                switch (cashFlowAccount.getMerchant()) {
                    case settlement:
                        return AccountType.MERCHANT_SETTLEMENT;
                    case guarantee:
                        return AccountType.MERCHANT_GUARANTEE;
                    case payout:
                        return AccountType.MERCHANT_PAYOUT;
                    default:
                        throw new IllegalArgumentException();
                }
            case PROVIDER:
                if (cashFlowAccount.getProvider() == ProviderCashFlowAccount.settlement) {
                    return AccountType.PROVIDER_SETTLEMENT;
                }
                throw new IllegalArgumentException();
            default:
                throw new IllegalArgumentException();
        }
    }

    private String buildCashFlowDescription(String payoutId, FinalCashFlowPosting finalCashFlowPosting) {
        String description = "PAYOUT-" + payoutId;
        if (finalCashFlowPosting.isSetDetails()) {
            description += ": " + finalCashFlowPosting.getDetails();
        }
        return description;
    }

    private String toPlanId(String payoutId) {
        return "payout_" + payoutId;
    }

    private String toRevertPlanId(String payoutId) {
        return "revert_" + toPlanId(payoutId);
    }
}
