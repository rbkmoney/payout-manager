package com.rbkmoney.payout.manager.service;

import com.rbkmoney.kafka.common.exception.KafkaProduceException;
import com.rbkmoney.payout.manager.Payout;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PayoutKafkaProducerService {

    private final KafkaTemplate<String, Payout> kafkaTemplate;

    @Value("${kafka.topic.payout.name}")
    private String topicName;

    @Value("${kafka.topic.payout.produce.enabled}")
    private boolean producerEnabled;

    public void send(Payout payout) {
        if (producerEnabled) {
            sendPayout(payout);
        }
    }

    private void sendPayout(Payout payout) {
        try {
            log.info("Try to send payout data to kafka: topicName={}, payoutId={}",
                    topicName, payout.getId());
            kafkaTemplate.send(topicName, payout.getId(), payout).get();
            log.info("Payout data to kafka was sent: topicName={}, payoutId={}",
                    topicName, payout.getId());
        } catch (InterruptedException e) {
            log.error("InterruptedException command: {}", payout, e);
            Thread.currentThread().interrupt();
            throw new KafkaProduceException(e);
        } catch (Exception e) {
            log.error("Error while sending command: {}", payout, e);
            throw new KafkaProduceException(e);
        }
    }
}
