package com.rbkmoney.payout.manager.service;

import com.rbkmoney.damsel.shumpune.AccounterSrv;
import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.damsel.shumpune.InvalidPostingParams;
import com.rbkmoney.damsel.shumpune.LatestClock;
import com.rbkmoney.payout.manager.config.PostgresqlSpringBootITest;
import com.rbkmoney.payout.manager.domain.tables.pojos.CashFlowPosting;
import com.rbkmoney.payout.manager.exception.AccounterException;
import com.rbkmoney.payout.manager.exception.NotFoundException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.List;
import java.util.stream.Collectors;

import static com.rbkmoney.payout.manager.util.ValuesGenerator.generatePayoutId;
import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.randomStreamOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@PostgresqlSpringBootITest
public class ShumwayServiceTest {

    @MockBean
    private AccounterSrv.Iface shumwayClient;

    @Autowired
    private CashFlowPostingService cashFlowPostingService;
    @Autowired
    private ShumwayService shumwayService;

    @Test
    public void shouldHold() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        Clock clock = Clock.latest(new LatestClock());
        when(shumwayClient.hold(any())).thenReturn(clock);
        assertEquals(
                clock,
                shumwayService.hold(payoutId, cashFlowPostings));
    }

    @Test
    public void shouldThrowExceptionAtHoldWhenClientIssue() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        when(shumwayClient.hold(any())).thenThrow(InvalidPostingParams.class);
        assertThrows(
                AccounterException.class,
                () -> shumwayService.hold(payoutId, cashFlowPostings));
    }

    @Test
    public void shouldCommit() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        Clock clock = Clock.latest(new LatestClock());
        when(shumwayClient.commitPlan(any())).thenReturn(clock);
        shumwayService.commit(payoutId);
        verify(shumwayClient, times(1)).commitPlan(any());
    }

    @Test
    public void shouldThrowExceptionAtCommitWhenCashFlowPostingIsNull() {
        String payoutId = generatePayoutId();
        assertThrows(
                NotFoundException.class,
                () -> shumwayService.commit(payoutId));
    }

    @Test
    public void shouldThrowExceptionAtCommitWhenClientIssue() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        when(shumwayClient.commitPlan(any())).thenThrow(InvalidPostingParams.class);
        assertThrows(
                AccounterException.class,
                () -> shumwayService.commit(payoutId));
    }

    @Test
    public void shouldRollback() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        Clock clock = Clock.latest(new LatestClock());
        when(shumwayClient.rollbackPlan(any())).thenReturn(clock);
        shumwayService.rollback(payoutId);
        verify(shumwayClient, times(1)).rollbackPlan(any());
    }

    @Test
    public void shouldThrowExceptionAtRollbackWhenCashFlowPostingIsNull() {
        String payoutId = generatePayoutId();
        assertThrows(
                NotFoundException.class,
                () -> shumwayService.rollback(payoutId));
    }

    @Test
    public void shouldThrowExceptionAtRollbackWhenClientIssue() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        when(shumwayClient.rollbackPlan(any())).thenThrow(InvalidPostingParams.class);
        assertThrows(
                AccounterException.class,
                () -> shumwayService.rollback(payoutId));
    }

    @Test
    public void shouldRevert() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        Clock clock = Clock.latest(new LatestClock());
        when(shumwayClient.hold(any())).thenReturn(clock);
        when(shumwayClient.commitPlan(any())).thenReturn(clock);
        shumwayService.revert(payoutId);
        verify(shumwayClient, times(0)).rollbackPlan(any());
        verify(shumwayClient, times(1)).hold(any());
        verify(shumwayClient, times(1)).commitPlan(any());
    }

    @Test
    public void shouldThrowExceptionAtRevertWhenCashFlowPostingIsNull() {
        String payoutId = generatePayoutId();
        assertThrows(
                NotFoundException.class,
                () -> shumwayService.revert(payoutId));
    }

    @Test
    public void shouldThrowExceptionAtRevertWhenClientIssue() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        when(shumwayClient.hold(any())).thenThrow(InvalidPostingParams.class);
        when(shumwayClient.commitPlan(any())).thenThrow(InvalidPostingParams.class);
        when(shumwayClient.rollbackPlan(any())).thenThrow(InvalidPostingParams.class);
        assertThrows(
                AccounterException.class,
                () -> shumwayService.revert(payoutId));
    }

    @Test
    public void shouldThrowExceptionAndRollbackAtRevertWhenHoldIssue() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        when(shumwayClient.hold(any())).thenThrow(InvalidPostingParams.class);
        Clock clock = Clock.latest(new LatestClock());
        when(shumwayClient.rollbackPlan(any())).thenReturn(clock);
        assertThrows(
                AccounterException.class,
                () -> shumwayService.revert(payoutId));
        verify(shumwayClient, times(1)).rollbackPlan(any());
    }

    @Test
    public void shouldThrowExceptionAndRollbackAtRevertWhenCommitIssue() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        Clock clock = Clock.latest(new LatestClock());
        when(shumwayClient.hold(any())).thenReturn(clock);
        when(shumwayClient.commitPlan(any())).thenThrow(InvalidPostingParams.class);
        when(shumwayClient.rollbackPlan(any())).thenReturn(clock);
        assertThrows(
                AccounterException.class,
                () -> shumwayService.revert(payoutId));
        verify(shumwayClient, times(1)).rollbackPlan(any());
    }
}
