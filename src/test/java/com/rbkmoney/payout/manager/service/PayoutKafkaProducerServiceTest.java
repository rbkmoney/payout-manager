package com.rbkmoney.payout.manager.service;

import com.rbkmoney.payout.manager.Event;
import com.rbkmoney.payout.manager.domain.tables.pojos.CashFlowPosting;
import com.rbkmoney.payout.manager.domain.tables.pojos.Payout;
import com.rbkmoney.payout.manager.util.ThriftUtil;
import com.rbkmoney.testcontainers.annotations.KafkaSpringBootTest;
import com.rbkmoney.testcontainers.annotations.kafka.KafkaTestcontainer;
import com.rbkmoney.testcontainers.annotations.kafka.config.KafkaConsumer;
import com.rbkmoney.testcontainers.annotations.postgresql.PostgresqlTestcontainerSingleton;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.random;
import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.randomStreamOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

@PostgresqlTestcontainerSingleton
@KafkaTestcontainer(
        properties = "kafka.topic.pm-events-payout.produce.enabled=true",
        topicsKeys = "kafka.topic.pm-events-payout.name")
@KafkaSpringBootTest
public class PayoutKafkaProducerServiceTest {

    private static final int TIMEOUT = 5;

    @Value("${kafka.topic.pm-events-payout.name}")
    private String topicName;

    @Autowired
    private PayoutKafkaProducerService payoutKafkaProducerService;

    @Autowired
    private KafkaConsumer<Event> testPayoutEventKafkaConsumer;

    @Test
    public void shouldProduceEvents() {
        int expected = 4;
        for (int i = 0; i < expected; i++) {
            Integer id = i + 1;
            Payout payout = random(Payout.class);
            payout.setPayoutId(String.valueOf(id));
            payout.setSequenceId(i);
            List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class)
                    .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payout.getPayoutId()))
                    .collect(Collectors.toList());
            Event event = ThriftUtil.createEvent(payout, cashFlowPostings);
            payoutKafkaProducerService.send(event);
        }
        List<Event> readEvents = new ArrayList<>();
        testPayoutEventKafkaConsumer.read(topicName, data -> readEvents.add(data.value()));
        Unreliables.retryUntilTrue(TIMEOUT, TimeUnit.SECONDS, () -> readEvents.size() == expected);
        for (int i = 0; i < expected; i++) {
            Integer id = i + 1;
            assertEquals(String.valueOf(id), readEvents.get(i).getPayoutId());
        }
    }
}
