package com.rbkmoney.payout.manager.dao.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.mapper.RecordRowMapper;
import com.rbkmoney.payout.manager.dao.CashFlowPostingDao;
import com.rbkmoney.payout.manager.domain.tables.pojos.CashFlowPosting;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

import static com.rbkmoney.payout.manager.domain.tables.CashFlowPosting.CASH_FLOW_POSTING;

@Component
public class CashFlowPostingDaoImpl extends AbstractGenericDao implements CashFlowPostingDao {

    private final RowMapper<CashFlowPosting> cashFlowPostingRowMapper;

    @Autowired
    public CashFlowPostingDaoImpl(HikariDataSource dataSource) {
        super(dataSource);
        cashFlowPostingRowMapper = new RecordRowMapper<>(CASH_FLOW_POSTING, CashFlowPosting.class);
    }

    @Override
    public void save(List<CashFlowPosting> cashFlowPostings) throws DaoException {
        List<Query> queries = cashFlowPostings.stream()
                .map(cashFlowPosting -> getDslContext().insertInto(CASH_FLOW_POSTING)
                        .set(getDslContext().newRecord(CASH_FLOW_POSTING, cashFlowPosting)))
                .collect(Collectors.toList());
        batchExecute(queries);
    }

    @Override
    public List<CashFlowPosting> getByPayoutId(String payoutId) throws DaoException {
        Query query = getDslContext().selectFrom(CASH_FLOW_POSTING)
                .where(CASH_FLOW_POSTING.PAYOUT_ID.eq(payoutId));
        return fetch(query, cashFlowPostingRowMapper);
    }
}
