package com.rbkmoney.payout.manager.config;

import com.rbkmoney.woody.api.flow.error.WUnavailableResultException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Collections;

@Configuration
public class RetryConfig {

    @Value("${retry-policy.maxAttempts}")
    private int maxAttempts;

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(
                new SimpleRetryPolicy(maxAttempts, Collections.singletonMap(WUnavailableResultException.class, true))
        );
        retryTemplate.setBackOffPolicy(new ExponentialBackOffPolicy());
        return retryTemplate;
    }
}
