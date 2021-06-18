package com.rbkmoney.payout.manager.dao;

import com.rbkmoney.payout.manager.AbstractDaoConfig;
import com.rbkmoney.payout.manager.domain.tables.pojos.Payout;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PayoutDaoTest extends AbstractDaoConfig {

    @Autowired
    private PayoutDao payoutDao;

    @Test
    public void testSaveAndGet()  {
        Payout payout = random(Payout.class, "id");
        payout.setPayoutId("1");
        payout.setId(payoutDao.save(payout));
        Payout second = new Payout(payout);
        second.setId(null);
        second.setPayoutId("2");
        second.setId(payoutDao.save(second));

        assertEquals(
                payout,
                payoutDao.get(payout.getPayoutId()));
        assertEquals(
                second,
                payoutDao.get(second.getPayoutId()));
    }
}
