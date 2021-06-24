package com.rbkmoney.payout.manager.service;

import com.rbkmoney.payout.manager.Event;
import com.rbkmoney.payout.manager.config.AbstractKafkaTest;
import com.rbkmoney.payout.manager.domain.tables.pojos.CashFlowPosting;
import com.rbkmoney.payout.manager.domain.tables.pojos.Payout;
import com.rbkmoney.payout.manager.util.PayoutEventDeserializer;
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
            payout.setSequenceId(i);
            List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class)
                    .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payout.getPayoutId()))
                    .collect(Collectors.toList());
            Event event = ThriftUtil.createEvent(payout, cashFlowPostings);
            payoutKafkaProducerService.send(event);
        }
        Consumer<String, Event> consumer = createConsumer(PayoutEventDeserializer.class);
        consumer.subscribe(List.of(topicName));
        ConsumerRecords<String, Event> poll = consumer.poll(Duration.ofMillis(5000));
        assertEquals(expected, poll.count());
        Iterable<ConsumerRecord<String, Event>> records = poll.records(topicName);
        List<Event> events = new ArrayList<>();
        records.forEach(consumerRecord -> events.add(consumerRecord.value()));
        for (int i = 0; i < expected; i++) {
            assertEquals(String.valueOf(i), events.get(i).getPayoutId());
        }
    }
}
