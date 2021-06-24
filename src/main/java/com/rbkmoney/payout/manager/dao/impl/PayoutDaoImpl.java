package com.rbkmoney.payout.manager.dao.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.mapper.RecordRowMapper;
import com.rbkmoney.payout.manager.dao.PayoutDao;
import com.rbkmoney.payout.manager.domain.enums.PayoutStatus;
import com.rbkmoney.payout.manager.domain.tables.pojos.Payout;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import static com.rbkmoney.payout.manager.domain.tables.Payout.PAYOUT;

@Component
public class PayoutDaoImpl extends AbstractGenericDao implements PayoutDao {

    private final RowMapper<Payout> payoutRowMapper;

    @Autowired
    public PayoutDaoImpl(HikariDataSource dataSource) {
        super(dataSource);
        payoutRowMapper = new RecordRowMapper<>(PAYOUT, Payout.class);
    }

    @Override
    public Payout get(String payoutId) throws DaoException {
        Query query = getDslContext().selectFrom(PAYOUT)
                .where(PAYOUT.PAYOUT_ID.eq(payoutId));

        return fetchOne(query, payoutRowMapper);
    }

    @Override
    public Payout getForUpdate(String payoutId) throws DaoException {
        Query query = getDslContext().selectFrom(PAYOUT)
                .where(PAYOUT.PAYOUT_ID.eq(payoutId))
                .forUpdate();

        return fetchOne(query, payoutRowMapper);
    }

    @Override
    public long save(Payout payout) throws DaoException {
        Query query = getDslContext().insertInto(PAYOUT)
                .set(getDslContext().newRecord(PAYOUT, payout))
                .returning(PAYOUT.ID);

        KeyHolder keyHolder = new GeneratedKeyHolder();
        executeOne(query, keyHolder);
        return keyHolder.getKey().longValue();
    }

    @Override
    public void changeStatus(String payoutId, PayoutStatus payoutStatus, String cancelDetails) throws DaoException {
        Query query = getDslContext().update(PAYOUT)
                .set(PAYOUT.STATUS, payoutStatus)
                .set(PAYOUT.SEQUENCE_ID, PAYOUT.SEQUENCE_ID.plus(1))
                .set(PAYOUT.CANCEL_DETAILS, cancelDetails)
                .where(PAYOUT.PAYOUT_ID.eq(payoutId));

        executeOne(query);
    }
}
