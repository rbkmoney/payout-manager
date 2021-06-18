package com.rbkmoney.payout.manager.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.payout.manager.exception.NotFoundException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

@Service
public class PartyManagementService {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final UserInfo userInfo = new UserInfo("admin", UserType.internal_user(new InternalUser()));

    private final PartyManagementSrv.Iface partyManagementClient;

    private final Cache<Map.Entry<String, PartyRevisionParam>, Party> partyCache;

    @Autowired
    public PartyManagementService(
            PartyManagementSrv.Iface partyManagementClient,
            @Value("${cache.maxSize}") long cacheMaximumSize
    ) {
        this.partyManagementClient = partyManagementClient;
        this.partyCache = Caffeine.newBuilder()
                .maximumSize(cacheMaximumSize)
                .build();
    }

    public Party getParty(String partyId, long partyRevision) throws NotFoundException {
        return getParty(partyId, PartyRevisionParam.revision(partyRevision));
    }

    public Party getParty(String partyId, PartyRevisionParam partyRevisionParam) throws NotFoundException {
        log.info("Trying to get party, partyId='{}', partyRevisionParam='{}'", partyId, partyRevisionParam);
        Party party = partyCache.get(
                new AbstractMap.SimpleEntry<>(partyId, partyRevisionParam),
                key -> {
                    try {
                        return partyManagementClient.checkout(userInfo, partyId, partyRevisionParam);
                    } catch (PartyNotFound ex) {
                        throw new NotFoundException(
                                String.format("Party not found, partyId='%s', partyRevisionParam='%s'",
                                        partyId, partyRevisionParam), ex);
                    } catch (InvalidPartyRevision ex) {
                        throw new NotFoundException(
                                String.format("Invalid party revision, partyId='%s', partyRevisionParam='%s'",
                                        partyId, partyRevisionParam), ex);
                    } catch (TException ex) {
                        throw new RuntimeException(
                                String.format("Failed to get party, partyId='%s', partyRevisionParam='%s'",
                                        partyId, partyRevisionParam), ex);
                    }
                });
        log.info("Party has been found, partyId='{}', partyRevisionParam='{}'", partyId, partyRevisionParam);
        return party;
    }

    public Contract getContract(String partyId, String contractId, PartyRevisionParam partyRevisionParam)
            throws NotFoundException {
        log.info("Trying to get contract, partyId='{}', contractId='{}', partyRevisionParam='{}'",
                partyId, contractId, partyRevisionParam);
        Party party = getParty(partyId, partyRevisionParam);

        Contract contract = party.getContracts().get(contractId);
        if (contract == null) {
            throw new NotFoundException(String.format("Shop not found, partyId='%s', contractId='%s', " +
                    "partyRevisionParam='%s'", partyId, contractId, partyRevisionParam));
        }
        log.info("Contract has been found, partyId='{}', contractId='{}', partyRevisionParam='{}'",
                partyId, contractId, partyRevisionParam);
        return contract;
    }

    public List<FinalCashFlowPosting> computePayoutCashFlow(
            String partyId,
            String shopId,
            Cash amount,
            String payoutToolId,
            String timestamp) throws NotFoundException {
        PayoutParams payoutParams = new PayoutParams(shopId, amount, timestamp)
                .setPayoutToolId(payoutToolId);
        return computePayoutCashFlow(partyId, payoutParams);
    }

    public List<FinalCashFlowPosting> computePayoutCashFlow(String partyId, PayoutParams payoutParams)
            throws NotFoundException {
        log.debug("Trying to compute payout cash flow, partyId='{}', payoutParams='{}'", partyId, payoutParams);
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

    public long getPartyRevision(String partyId) throws NotFoundException {
        log.info("Trying to get party revision, partyId='{}'", partyId);
        try {
            long revision = partyManagementClient.getRevision(userInfo, partyId);
            log.info("Party revision has been found, partyId='{}', revision='{}'", partyId, revision);
            return revision;
        } catch (PartyNotFound ex) {
            throw new NotFoundException(String.format("Party not found, partyId='%s'", partyId), ex);
        } catch (TException ex) {
            throw new RuntimeException(String.format("Failed to get party revision, partyId='%s'", partyId), ex);
        }
    }
}
