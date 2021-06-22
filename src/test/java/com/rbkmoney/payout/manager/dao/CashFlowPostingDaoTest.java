package com.rbkmoney.payout.manager.dao;

import com.rbkmoney.payout.manager.config.AbstractDaoConfig;
import com.rbkmoney.payout.manager.domain.tables.pojos.CashFlowPosting;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.stream.Collectors;

import static io.github.benas.randombeans.api.EnhancedRandom.randomStreamOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CashFlowPostingDaoTest extends AbstractDaoConfig {

    @Autowired
    private CashFlowPostingDao cashFlowPostingDao;

    @Test
    public void testSaveAndGet() {
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId("1"))
                .collect(Collectors.toList());
        cashFlowPostingDao.save(cashFlowPostings);
        List<CashFlowPosting> seconds = randomStreamOf(5, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId("2"))
                .collect(Collectors.toList());
        cashFlowPostingDao.save(seconds);
        assertEquals(
                cashFlowPostings.size(),
                cashFlowPostingDao.getByPayoutId("1").size());
        assertEquals(
                seconds.size(),
                cashFlowPostingDao.getByPayoutId("2").size());
    }
}
