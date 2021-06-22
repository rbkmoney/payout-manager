package com.rbkmoney.payout.manager.kafka;

import com.rbkmoney.kafka.common.serialization.AbstractThriftDeserializer;
import com.rbkmoney.payout.manager.Payout;

public class PayoutDeserializer extends AbstractThriftDeserializer<Payout> {

    @Override
    public Payout deserialize(String s, byte[] bytes) {
        return super.deserialize(bytes, new Payout());
    }
}
