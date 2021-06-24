package com.rbkmoney.payout.manager.config;

import com.rbkmoney.payout.manager.PayoutManagerApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = PayoutManagerApplication.class,
        initializers = AbstractDaoConfig.Initializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource("classpath:application.yml")
@Testcontainers
public abstract class AbstractDaoConfig {

    @Container
    public static PostgreSQLContainer postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:9.6"));

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues.of(
                    "spring.datasource.url=" + postgres.getJdbcUrl(),
                    "spring.datasource.username=" + postgres.getUsername(),
                    "spring.datasource.password=" + postgres.getPassword(),
                    "flyway.url=" + postgres.getJdbcUrl(),
                    "flyway.user=" + postgres.getUsername(),
                    "flyway.password=" + postgres.getPassword()
            ).applyTo(configurableApplicationContext);
        }
    }

    @LocalServerPort
    protected int port;

    public static String generatePayoutId() {
        return UUID.randomUUID().toString();
    }
}
