package com.rbkmoney.payout.manager.dao;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.payout.manager.domain.tables.pojos.CashFlowPosting;

import java.util.List;

public interface CashFlowPostingDao {

    void save(List<CashFlowPosting> cashFlowPostings) throws DaoException;

    List<CashFlowPosting> getByPayoutId(String payoutId) throws DaoException;

}
