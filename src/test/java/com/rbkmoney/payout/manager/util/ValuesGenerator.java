package com.rbkmoney.payout.manager.util;

import java.util.UUID;

public class ValuesGenerator {

    public static String generatePayoutId() {
        return UUID.randomUUID().toString();
    }
}
