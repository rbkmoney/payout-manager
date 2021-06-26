package com.rbkmoney.payout.manager.config;

import com.rbkmoney.payout.manager.PayoutManagerApplication;
import com.rbkmoney.payout.manager.util.PayoutEventDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@Slf4j
@SpringBootTest(webEnvironment = RANDOM_PORT)
@ContextConfiguration(classes = PayoutManagerApplication.class,
        initializers = AbstractKafkaTest.Initializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource("classpath:application.yml")
@Testcontainers
public abstract class AbstractKafkaTest extends AbstractDaoConfig {

    private static final String CONFLUENT_IMAGE_NAME = "confluentinc/cp-kafka";
    private static final String CONFLUENT_PLATFORM_VERSION = "latest";

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse(CONFLUENT_IMAGE_NAME)
            .withTag(CONFLUENT_PLATFORM_VERSION))
            .withEmbeddedZookeeper();

    public static class Initializer extends ConfigDataApplicationContextInitializer {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            TestPropertyValues
                    .of("kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                            "kafka.ssl.enabled=false",
                            "kafka.topic.pm-events-payout.produce.enabled=true")
                    .applyTo(applicationContext);
            initTopic(
                    applicationContext.getEnvironment()
                            .getProperty("kafka.topic.pm-events-payout.name"));
        }

        private <T> Consumer<String, T> initTopic(String topicName) {
            Consumer<String, T> consumer = createConsumer(PayoutEventDeserializer.class);
            try {
                consumer.subscribe(Collections.singletonList(topicName));
                consumer.poll(Duration.ofMillis(100L));
            } catch (Exception e) {
                log.error("KafkaAbstractTest initialize e: ", e);
            }
            consumer.close();
            return consumer;
        }
    }

    public static <T> Consumer<String, T> createConsumer(Class clazz) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, clazz);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }
}
