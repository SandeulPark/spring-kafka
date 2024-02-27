package org.example.springkafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "retryListener.startup=false"
})
@EmbeddedKafka(topics = {"my-topic"}, partitions = 3)
@SpringBootTest
public class ConsumerTest {

    @Autowired
    EmbeddedKafkaBroker broker;
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;
    @Autowired
    FailureRepository failureRepository;
    @SpyBean
    MyConsumer myConsumerSpy;
    @SpyBean
    MyService myServiceSpy;
    private Consumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        // 테스트를 실행하기 전 모든 리스너가 준비될 때까지 대기
        // AutoStartup이 true인 컨테이너만 준비될 때까지 대기한다.
        endpointRegistry.getAllListenerContainers().forEach(messageListenerContainer -> {
            if (messageListenerContainer.isAutoStartup())
                ContainerTestUtils.waitForAssignment(messageListenerContainer, broker.getPartitionsPerTopic());
        });
    }

    @DisplayName("컨슘 실패 시 재시도 후 실패 로그에 기록한다.")
    @Test
    void retryAndRecover() throws Exception {
        // Given
        String json = "{\"name\": \"sandro\", \"failure\": \"true\"}";
        kafkaTemplate.send("my-topic", json).get();

        // When
        new CountDownLatch(1).await(3000, TimeUnit.MILLISECONDS);

        // Then
        verify(myConsumerSpy, times(2)).onMessage(isA(ConsumerRecord.class));
        verify(myServiceSpy, times(2)).save(isA(ConsumerRecord.class));

        List<FailureEntity> failureRecords = failureRepository.findAll();
        assertThat(failureRecords).hasSize(1);
        System.out.println("failureRecord" + failureRecords);
    }
}
