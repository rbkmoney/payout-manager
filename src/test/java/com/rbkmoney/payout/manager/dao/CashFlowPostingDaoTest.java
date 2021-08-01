package com.rbkmoney.payout.manager.dao;

import com.rbkmoney.payout.manager.config.PostgresqlSpringBootITest;
import com.rbkmoney.payout.manager.domain.tables.pojos.CashFlowPosting;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.stream.Collectors;

import static com.rbkmoney.payout.manager.util.ValuesGenerator.generatePayoutId;
import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.randomStreamOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

@PostgresqlSpringBootITest
public class CashFlowPostingDaoTest {

    @Autowired
    private CashFlowPostingDao cashFlowPostingDao;

    @Test
    public void testSaveAndGet() {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingDao.save(cashFlowPostings);
        String second = generatePayoutId();
        List<CashFlowPosting> seconds = randomStreamOf(5, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(second))
                .collect(Collectors.toList());
        cashFlowPostingDao.save(seconds);
        assertEquals(
                cashFlowPostings.size(),
                cashFlowPostingDao.getByPayoutId(payoutId).size());
        assertEquals(
                seconds.size(),
                cashFlowPostingDao.getByPayoutId(second).size());
    }
}
