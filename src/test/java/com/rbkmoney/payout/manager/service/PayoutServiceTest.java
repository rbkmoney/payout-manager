package com.rbkmoney.payout.manager.service;

import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.shumpune.Balance;
import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.damsel.shumpune.LatestClock;
import com.rbkmoney.geck.serializer.kit.mock.MockMode;
import com.rbkmoney.geck.serializer.kit.mock.MockTBaseProcessor;
import com.rbkmoney.geck.serializer.kit.tbase.TBaseHandler;
import com.rbkmoney.payout.manager.config.AbstractDaoConfig;
import com.rbkmoney.payout.manager.domain.enums.PayoutStatus;
import com.rbkmoney.payout.manager.domain.tables.pojos.Payout;
import com.rbkmoney.payout.manager.exception.InsufficientFundsException;
import com.rbkmoney.payout.manager.exception.InvalidStateException;
import com.rbkmoney.payout.manager.exception.NotFoundException;
import lombok.SneakyThrows;
import org.apache.thrift.TBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class PayoutServiceTest extends AbstractDaoConfig {

    @MockBean
    private ShumwayService shumwayService;
    @MockBean
    private PartyManagementService partyManagementService;

    @Autowired
    private CashFlowPostingService cashFlowPostingService;
    @Autowired
    private PayoutService payoutService;

    private MockTBaseProcessor mockTBaseProcessor;

    @BeforeEach
    public void setUp() {
        mockTBaseProcessor = new MockTBaseProcessor(MockMode.ALL, 15, 1);
        mockTBaseProcessor.addFieldHandler(
                structHandler -> structHandler.value(Instant.now().toString()),
                "created_at", "at", "due");
    }

    @Test
    public void shouldCreateAndSave() {
        String partyId = "partyId";
        Party party = new Party();
        Party returnedParty = fillTBaseObject(party, Party.class);
        returnedParty.setId(partyId);
        String shopId = "shopId";
        Shop shop = new Shop();
        Shop returnedShop = fillTBaseObject(shop, Shop.class);
        returnedShop.setId(shopId);
        returnedParty.setShops(Map.of(shopId, returnedShop));
        when(partyManagementService.getParty(eq(partyId))).thenReturn(returnedParty);
        FinalCashFlowPosting finalCashFlowPosting = new FinalCashFlowPosting();
        FinalCashFlowPosting returnedPayoutAmount = fillTBaseObject(finalCashFlowPosting, FinalCashFlowPosting.class);
        returnedPayoutAmount.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedPayoutAmount.getDestination().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.payout));
        returnedPayoutAmount.getVolume().setAmount(5L);
        FinalCashFlowPosting returnedPayoutFixedFee = new FinalCashFlowPosting(returnedPayoutAmount);
        returnedPayoutFixedFee.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.payout));
        returnedPayoutFixedFee.getDestination().setAccountType(
                CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedPayoutFixedFee.getVolume().setAmount(1L);
        FinalCashFlowPosting returnedFee = new FinalCashFlowPosting(returnedPayoutAmount);
        returnedFee.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedFee.getDestination().setAccountType(CashFlowAccount.system(SystemCashFlowAccount.settlement));
        returnedFee.getVolume().setAmount(1L);
        when(partyManagementService.computePayoutCashFlow(
                eq(partyId),
                eq(shopId),
                any(),
                anyString(),
                anyString()))
                .thenReturn(List.of(returnedPayoutAmount, returnedPayoutFixedFee, returnedFee));
        when(shumwayService.hold(anyString(), anyList())).thenReturn(Clock.latest(new LatestClock()));
        Balance balance = new Balance();
        Balance returnedBalance = fillTBaseObject(balance, Balance.class);
        returnedBalance.setMinAvailableAmount(1L);
        when(shumwayService.getBalance(any(), any(), anyString())).thenReturn(returnedBalance);
        Payout payout = payoutService.create(
                partyId,
                shopId,
                new Cash(100L, new CurrencyRef("RUB")));
        assertTrue(new ReflectionEquals(payout, "id").matches(payoutService.get(payout.getPayoutId())));
        assertEquals(4L, payout.getAmount());
        assertEquals(2L, payout.getFee());
        assertEquals(PayoutStatus.UNPAID, payout.getStatus());
        assertEquals(returnedParty.getShops().get(shopId).getPayoutToolId(), payout.getPayoutToolId());
        assertEquals(3L, cashFlowPostingService.getCashFlowPostings(payout.getPayoutId()).size());
        assertNotNull(cashFlowPostingService.getCashFlowPostings(payout.getPayoutId()).stream()
                .filter(cashFlowPosting ->
                        cashFlowPosting.getToAccountId().equals(returnedFee.getDestination().getAccountId()))
                .findFirst()
                .orElse(null));
    }

    @Test
    public void shouldThrowExceptionAtCreateWhenPartyManagementNotFound() {
        String partyId = "partyId";
        Party party = new Party();
        Party returnedParty = fillTBaseObject(party, Party.class);
        returnedParty.setId(partyId);
        String shopId = "shopId";
        Shop shop = new Shop();
        Shop returnedShop = fillTBaseObject(shop, Shop.class);
        returnedShop.setId(shopId);
        returnedParty.setShops(Map.of(shopId, returnedShop));
        when(partyManagementService.getParty(eq(partyId))).thenThrow(NotFoundException.class);
        assertThrows(
                NotFoundException.class,
                () -> payoutService.create(
                        partyId,
                        shopId,
                        new Cash(100L, new CurrencyRef("RUB"))));
        when(partyManagementService.getParty(eq(partyId))).thenReturn(returnedParty);
        when(partyManagementService.computePayoutCashFlow(
                eq(partyId),
                eq(shopId),
                any(),
                anyString(),
                anyString()))
                .thenThrow(NotFoundException.class);
        assertThrows(
                NotFoundException.class,
                () -> payoutService.create(
                        partyId,
                        shopId,
                        new Cash(100L, new CurrencyRef("RUB"))));
    }

    @Test
    public void shouldThrowExceptionAtCreateWhenComputedAmountIsNull() {
        String partyId = "partyId";
        Party party = new Party();
        Party returnedParty = fillTBaseObject(party, Party.class);
        returnedParty.setId(partyId);
        String shopId = "shopId";
        Shop shop = new Shop();
        Shop returnedShop = fillTBaseObject(shop, Shop.class);
        returnedShop.setId(shopId);
        returnedParty.setShops(Map.of(shopId, returnedShop));
        when(partyManagementService.getParty(eq(partyId))).thenReturn(returnedParty);
        FinalCashFlowPosting finalCashFlowPosting = new FinalCashFlowPosting();
        FinalCashFlowPosting returnedPayoutAmount = fillTBaseObject(finalCashFlowPosting, FinalCashFlowPosting.class);
        returnedPayoutAmount.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedPayoutAmount.getDestination().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.payout));
        returnedPayoutAmount.getVolume().setAmount(1L);
        FinalCashFlowPosting returnedPayoutFixedFee = new FinalCashFlowPosting(returnedPayoutAmount);
        returnedPayoutFixedFee.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.payout));
        returnedPayoutFixedFee.getDestination().setAccountType(
                CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedPayoutFixedFee.getVolume().setAmount(1L);
        FinalCashFlowPosting returnedFee = new FinalCashFlowPosting(returnedPayoutAmount);
        returnedFee.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedFee.getDestination().setAccountType(CashFlowAccount.system(SystemCashFlowAccount.settlement));
        returnedFee.getVolume().setAmount(1L);
        when(partyManagementService.computePayoutCashFlow(
                eq(partyId),
                eq(shopId),
                any(),
                anyString(),
                anyString()))
                .thenReturn(List.of(returnedPayoutAmount, returnedPayoutFixedFee, returnedFee));
        assertThrows(
                InsufficientFundsException.class,
                () -> payoutService.create(
                        partyId,
                        shopId,
                        new Cash(100L, new CurrencyRef("RUB"))));
    }

    @Test
    public void shouldThrowExceptionAtCreateWhenBalanceAmountIsNegative() {
        String partyId = "partyId";
        Party party = new Party();
        Party returnedParty = fillTBaseObject(party, Party.class);
        returnedParty.setId(partyId);
        String shopId = "shopId";
        Shop shop = new Shop();
        Shop returnedShop = fillTBaseObject(shop, Shop.class);
        returnedShop.setId(shopId);
        returnedParty.setShops(Map.of(shopId, returnedShop));
        when(partyManagementService.getParty(eq(partyId))).thenReturn(returnedParty);
        FinalCashFlowPosting finalCashFlowPosting = new FinalCashFlowPosting();
        FinalCashFlowPosting returnedPayoutAmount = fillTBaseObject(finalCashFlowPosting, FinalCashFlowPosting.class);
        returnedPayoutAmount.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedPayoutAmount.getDestination().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.payout));
        returnedPayoutAmount.getVolume().setAmount(5L);
        FinalCashFlowPosting returnedPayoutFixedFee = new FinalCashFlowPosting(returnedPayoutAmount);
        returnedPayoutFixedFee.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.payout));
        returnedPayoutFixedFee.getDestination().setAccountType(
                CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedPayoutFixedFee.getVolume().setAmount(1L);
        FinalCashFlowPosting returnedFee = new FinalCashFlowPosting(returnedPayoutAmount);
        returnedFee.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedFee.getDestination().setAccountType(CashFlowAccount.system(SystemCashFlowAccount.settlement));
        returnedFee.getVolume().setAmount(1L);
        when(partyManagementService.computePayoutCashFlow(
                eq(partyId),
                eq(shopId),
                any(),
                anyString(),
                anyString()))
                .thenReturn(List.of(returnedPayoutAmount, returnedPayoutFixedFee, returnedFee));
        when(shumwayService.hold(anyString(), anyList())).thenReturn(Clock.latest(new LatestClock()));
        when(shumwayService.getBalance(any(), any(), anyString())).thenReturn(null);
        doNothing().when(shumwayService).rollback(anyString());
        assertThrows(
                InsufficientFundsException.class,
                () -> payoutService.create(
                        partyId,
                        shopId,
                        new Cash(100L, new CurrencyRef("RUB"))));
    }

    @Test
    public void shouldSaveAndGet() {
        Payout payout = random(Payout.class);
        Payout savedPayout = payoutService.save(
                payout.getPayoutId(),
                payout.getCreatedAt(),
                payout.getPartyId(),
                payout.getShopId(),
                payout.getPayoutToolId(),
                payout.getAmount(),
                payout.getFee(),
                payout.getCurrencyCode());
        assertTrue(new ReflectionEquals(savedPayout, "id")
                .matches(payoutService.get(payout.getPayoutId())));
        assertEquals(PayoutStatus.UNPAID, payoutService.get(payout.getPayoutId()).getStatus());
    }

    @Test
    public void shouldThrowExceptionAtGetWhenPayoutNotFound() {
        assertThrows(
                NotFoundException.class,
                () -> payoutService.get(generatePayoutId()));
    }

    @Test
    public void shouldConfirm() {
        Payout payout = random(Payout.class);
        payoutService.save(
                payout.getPayoutId(),
                payout.getCreatedAt(),
                payout.getPartyId(),
                payout.getShopId(),
                payout.getPayoutToolId(),
                payout.getAmount(),
                payout.getFee(),
                payout.getCurrencyCode());
        doNothing().when(shumwayService).commit(anyString());
        payoutService.confirm(payout.getPayoutId());
        assertEquals(PayoutStatus.CONFIRMED, payoutService.get(payout.getPayoutId()).getStatus());
        verify(shumwayService, times(1)).commit(anyString());
        payoutService.confirm(payout.getPayoutId());
        assertEquals(PayoutStatus.CONFIRMED, payoutService.get(payout.getPayoutId()).getStatus());
        verify(shumwayService, times(1)).commit(anyString());
    }

    @Test
    public void shouldThrowExceptionAtConfirmWhenPayoutNotFound() {
        assertThrows(
                NotFoundException.class,
                () -> payoutService.confirm(generatePayoutId()));
    }

    @Test
    public void shouldThrowExceptionAtConfirmWhenStateIsCancelled() {
        Payout payout = random(Payout.class);
        payoutService.save(
                payout.getPayoutId(),
                payout.getCreatedAt(),
                payout.getPartyId(),
                payout.getShopId(),
                payout.getPayoutToolId(),
                payout.getAmount(),
                payout.getFee(),
                payout.getCurrencyCode());
        doNothing().when(shumwayService).commit(anyString());
        doNothing().when(shumwayService).rollback(anyString());
        payoutService.cancel(payout.getPayoutId());
        assertEquals(PayoutStatus.CANCELLED, payoutService.get(payout.getPayoutId()).getStatus());
        assertThrows(
                InvalidStateException.class,
                () -> payoutService.confirm(payout.getPayoutId()));
    }

    @Test
    public void shouldCancel() {
        Payout payout = random(Payout.class);
        payoutService.save(
                payout.getPayoutId(),
                payout.getCreatedAt(),
                payout.getPartyId(),
                payout.getShopId(),
                payout.getPayoutToolId(),
                payout.getAmount(),
                payout.getFee(),
                payout.getCurrencyCode());
        doNothing().when(shumwayService).rollback(anyString());
        payoutService.cancel(payout.getPayoutId());
        assertEquals(PayoutStatus.CANCELLED, payoutService.get(payout.getPayoutId()).getStatus());
        verify(shumwayService, times(1)).rollback(anyString());
        verify(shumwayService, times(0)).revert(anyString());
        payoutService.cancel(payout.getPayoutId());
        assertEquals(PayoutStatus.CANCELLED, payoutService.get(payout.getPayoutId()).getStatus());
        verify(shumwayService, times(1)).rollback(anyString());
        verify(shumwayService, times(0)).revert(anyString());
    }

    @Test
    public void shouldCancelAfterConfirm() {
        Payout payout = random(Payout.class);
        payoutService.save(
                payout.getPayoutId(),
                payout.getCreatedAt(),
                payout.getPartyId(),
                payout.getShopId(),
                payout.getPayoutToolId(),
                payout.getAmount(),
                payout.getFee(),
                payout.getCurrencyCode());
        doNothing().when(shumwayService).commit(anyString());
        payoutService.confirm(payout.getPayoutId());
        assertEquals(PayoutStatus.CONFIRMED, payoutService.get(payout.getPayoutId()).getStatus());
        doNothing().when(shumwayService).revert(anyString());
        payoutService.cancel(payout.getPayoutId());
        assertEquals(PayoutStatus.CANCELLED, payoutService.get(payout.getPayoutId()).getStatus());
        verify(shumwayService, times(0)).rollback(anyString());
        verify(shumwayService, times(1)).revert(anyString());
    }

    @Test
    public void shouldThrowExceptionAtCancelWhenPayoutNotFound() {
        assertThrows(
                NotFoundException.class,
                () -> payoutService.cancel(generatePayoutId()));
    }

    @SneakyThrows
    public <T extends TBase> T fillTBaseObject(T value, Class<T> type) {
        return mockTBaseProcessor.process(value, new TBaseHandler<>(type));
    }
}
