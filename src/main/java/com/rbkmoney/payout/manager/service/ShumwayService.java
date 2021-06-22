package com.rbkmoney.payout.manager.service;

import com.rbkmoney.damsel.base.InvalidRequest;
import com.rbkmoney.damsel.shumpune.*;
import com.rbkmoney.payout.manager.domain.tables.pojos.CashFlowPosting;
import com.rbkmoney.payout.manager.exception.AccounterException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShumwayService {

    private final AccounterSrv.Iface shumwayClient;
    private final RetryTemplate retryTemplate;
    private final CashFlowPostingService cashFlowPostingService;

    public Clock hold(String payoutId, List<CashFlowPosting> cashFlowPostings) {
        log.debug("Trying to hold payout postings, payoutId='{}', cashFlowPostings='{}'",
                payoutId, cashFlowPostings);
        try {
            String postingPlanId = toPlanId(payoutId);
            PostingBatch postingBatch = toPostingBatch(cashFlowPostings);
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
        List<CashFlowPosting> cashFlowPostings = cashFlowPostingService.getCashFlowPostings(payoutId);
        try {
            String postingPlanId = toPlanId(payoutId);
            List<PostingBatch> postingBatches = List.of(toPostingBatch(cashFlowPostings));
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
        List<CashFlowPosting> cashFlowPostings = cashFlowPostingService.getCashFlowPostings(payoutId);
        try {
            String postingPlanId = toPlanId(payoutId);
            List<PostingBatch> postingBatches = List.of(toPostingBatch(cashFlowPostings));
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
        List<CashFlowPosting> cashFlowPostings = cashFlowPostingService.getCashFlowPostings(payoutId);
        try {
            String revertPlanId = toRevertPlanId(payoutId);
            PostingBatch revertPostingBatch = revertPostingBatch(
                    toPostingBatch(cashFlowPostings),
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
            PostingBatch postingBatch,
            Function<Posting, Posting> howToRevert) {
        return new PostingBatch(
                postingBatch.getId(),
                postingBatch.getPostings().stream()
                        .map(howToRevert)
                        .collect(Collectors.toList()));
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

    private PostingBatch toPostingBatch(List<CashFlowPosting> postings) {
        return new PostingBatch(
                1L,
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
        posting.setDescription(buildPostingDescription(
                cashFlowPosting.getPayoutId(),
                cashFlowPosting.getDescription()));
        return posting;
    }

    private String buildPostingDescription(String payoutId, String description) {
        String postingDescription = "PAYOUT-" + payoutId;
        if (description != null) {
            postingDescription += ": " + description;
        }
        return postingDescription;
    }

    private String toPlanId(String payoutId) {
        return "payout_" + payoutId;
    }

    private String toRevertPlanId(String payoutId) {
        return "revert_" + toPlanId(payoutId);
    }
}
