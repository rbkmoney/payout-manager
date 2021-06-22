package com.rbkmoney.payout.manager.service;

import com.rbkmoney.payout.manager.config.AbstractKafkaTest;
import com.rbkmoney.payout.manager.domain.tables.pojos.CashFlowPosting;
import com.rbkmoney.payout.manager.domain.tables.pojos.Payout;
import com.rbkmoney.payout.manager.kafka.PayoutDeserializer;
import com.rbkmoney.payout.manager.util.ThriftUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static io.github.benas.randombeans.api.EnhancedRandom.randomStreamOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PayoutKafkaProducerServiceTest extends AbstractKafkaTest {

    @Value("${kafka.topic.payout.name}")
    private String topicName;

    @Autowired
    private PayoutKafkaProducerService payoutKafkaProducerService;

    @Test
    public void shouldProducePayouts() {
        int expected = 4;
        for (int i = 0; i < expected; i++) {
            Payout payout = random(Payout.class);
            payout.setPayoutId(String.valueOf(i));
            List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class)
                    .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payout.getPayoutId()))
                    .collect(Collectors.toList());
            var thriftPayout = ThriftUtil.toThriftPayout(payout, cashFlowPostings);
            payoutKafkaProducerService.send(thriftPayout);
        }
        Consumer<String, com.rbkmoney.payout.manager.Payout> consumer = createConsumer(PayoutDeserializer.class);
        consumer.subscribe(List.of(topicName));
        ConsumerRecords<String, com.rbkmoney.payout.manager.Payout> poll = consumer.poll(Duration.ofMillis(5000));
        assertEquals(expected, poll.count());
        Iterable<ConsumerRecord<String, com.rbkmoney.payout.manager.Payout>> records = poll.records(topicName);
        List<com.rbkmoney.payout.manager.Payout> thriftPayouts = new ArrayList<>();
        records.forEach(consumerRecord -> thriftPayouts.add(consumerRecord.value()));
        for (int i = 0; i < expected; i++) {
            assertEquals(String.valueOf(i), thriftPayouts.get(i).getId());
        }
    }
}
