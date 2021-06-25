package com.rbkmoney.payout.manager.handler;

import com.rbkmoney.damsel.base.InvalidRequest;
import com.rbkmoney.damsel.domain.Cash;
import com.rbkmoney.damsel.domain.CurrencyRef;
import com.rbkmoney.kafka.common.exception.KafkaProduceException;
import com.rbkmoney.payout.manager.InsufficientFunds;
import com.rbkmoney.payout.manager.PayoutParams;
import com.rbkmoney.payout.manager.ShopParams;
import com.rbkmoney.payout.manager.domain.tables.pojos.CashFlowPosting;
import com.rbkmoney.payout.manager.domain.tables.pojos.Payout;
import com.rbkmoney.payout.manager.exception.InsufficientFundsException;
import com.rbkmoney.payout.manager.exception.InvalidRequestException;
import com.rbkmoney.payout.manager.service.CashFlowPostingService;
import com.rbkmoney.payout.manager.service.PayoutKafkaProducerService;
import com.rbkmoney.payout.manager.service.PayoutService;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.stream.Collectors;

import static com.rbkmoney.payout.manager.config.AbstractDaoConfig.generatePayoutId;
import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static io.github.benas.randombeans.api.EnhancedRandom.randomStreamOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(
        classes =
                PayoutManagementHandler.class,
        initializers = PayoutManagementHandlerTest.Initializer.class
)
@TestPropertySource("classpath:application.yml")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Slf4j
public class PayoutManagementHandlerTest {

    @MockBean
    private PayoutService payoutService;
    @MockBean
    private CashFlowPostingService cashFlowPostingService;
    @MockBean
    private PayoutKafkaProducerService payoutKafkaProducerService;

    @Autowired
    private PayoutManagementHandler payoutManagementHandler;

    @Test
    public void shouldCreate() throws TException {
        String payoutId = generatePayoutId();
        when(payoutService.create(anyString(), anyString(), any())).thenReturn(payoutId);
        Payout payout = random(Payout.class);
        when(payoutService.get(eq(payoutId))).thenReturn(payout);
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        when(cashFlowPostingService.getCashFlowPostings(anyString())).thenReturn(cashFlowPostings);
        doNothing().when(payoutKafkaProducerService).send(any());
        PayoutParams payoutParams = new PayoutParams(
                new ShopParams("partyId", "shopId"),
                new Cash(100L, new CurrencyRef("RUB")));
        assertNotNull(payoutManagementHandler.createPayout(payoutParams));
    }

    @Test
    public void shouldThrowExceptionAtCreateWhenInsufficientFundsIssue() {
        when(payoutService.create(anyString(), anyString(), any())).thenThrow(InsufficientFundsException.class);
        PayoutParams payoutParams = new PayoutParams(
                new ShopParams("partyId", "shopId"),
                new Cash(100L, new CurrencyRef("RUB")));
        assertThrows(
                InsufficientFunds.class,
                () -> payoutManagementHandler.createPayout(payoutParams));
    }

    @Test
    public void shouldThrowExceptionAtCreateWhenPayoutToolIdIsNull() {
        when(payoutService.create(anyString(), anyString(), any())).thenThrow(InvalidRequestException.class);
        PayoutParams payoutParams = new PayoutParams(
                new ShopParams("partyId", "shopId"),
                new Cash(100L, new CurrencyRef("RUB")));
        assertThrows(
                InvalidRequest.class,
                () -> payoutManagementHandler.createPayout(payoutParams));
    }

    @Test
    public void shouldThrowExceptionAtCreateWhenKafkaIssue() {
        String payoutId = generatePayoutId();
        when(payoutService.create(anyString(), anyString(), any())).thenReturn(payoutId);
        Payout payout = random(Payout.class);
        when(payoutService.get(eq(payoutId))).thenReturn(payout);
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        when(cashFlowPostingService.getCashFlowPostings(anyString())).thenReturn(cashFlowPostings);
        doThrow(KafkaProduceException.class).when(payoutKafkaProducerService).send(any());
        PayoutParams payoutParams = new PayoutParams(
                new ShopParams("partyId", "shopId"),
                new Cash(100L, new CurrencyRef("RUB")));
        assertThrows(
                KafkaProduceException.class,
                () -> payoutManagementHandler.createPayout(payoutParams));
    }

    public static class Initializer extends ConfigDataApplicationContextInitializer {

        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            super.initialize(configurableApplicationContext);
        }
    }
}
