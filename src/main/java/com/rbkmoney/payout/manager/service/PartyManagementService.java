package com.rbkmoney.payout.manager.service;

import com.rbkmoney.damsel.domain.Cash;
import com.rbkmoney.damsel.domain.FinalCashFlowPosting;
import com.rbkmoney.damsel.domain.Party;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.payout.manager.exception.NotFoundException;
import lombok.RequiredArgsConstructor;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class PartyManagementService {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final UserInfo userInfo = new UserInfo("admin", UserType.internal_user(new InternalUser()));

    private final PartyManagementSrv.Iface partyManagementClient;

    public Party getParty(String partyId) throws NotFoundException {
        log.info("Trying to get party, partyId='{}'", partyId);
        try {
            Party party = partyManagementClient.get(userInfo, partyId);
            log.info("Party has been found, partyId='{}'", partyId);
            return party;
        } catch (PartyNotFound ex) {
            throw new NotFoundException(
                    String.format("Party not found, partyId='%s'", partyId), ex);
        } catch (InvalidPartyRevision ex) {
            throw new NotFoundException(
                    String.format("Invalid party revision, partyId='%s'", partyId), ex);
        } catch (TException ex) {
            throw new RuntimeException(
                    String.format("Failed to get party, partyId='%s'", partyId), ex);
        }
    }

    public List<FinalCashFlowPosting> computePayoutCashFlow(
            String partyId,
            String shopId,
            Cash amount,
            String payoutToolId,
            String timestamp) throws NotFoundException {
        log.debug("Trying to compute payout cash flow, partyId='{}'", partyId);
        PayoutParams payoutParams = new PayoutParams(shopId, amount, timestamp)
                .setPayoutToolId(payoutToolId);
        try {
            var finalCashFlowPostings = partyManagementClient.computePayoutCashFlow(
                    userInfo,
                    partyId,
                    payoutParams);
            log.info("Payout cash flow has been computed, partyId='{}', payoutParams='{}', postings='{}'",
                    partyId, payoutParams, finalCashFlowPostings);
            return finalCashFlowPostings;
        } catch (PartyNotFound | PartyNotExistsYet | ShopNotFound | PayoutToolNotFound ex) {
            throw new NotFoundException(String.format("%s, partyId='%s', payoutParams='%s'",
                    ex.getClass().getSimpleName(), partyId, payoutParams), ex);
        } catch (TException ex) {
            throw new RuntimeException(String.format("Failed to compute payout cash flow, partyId='%s', " +
                    "payoutParams='%s'", partyId, payoutParams), ex);
        }
    }
}
