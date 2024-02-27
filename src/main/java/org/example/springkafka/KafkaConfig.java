package org.example.springkafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class KafkaConfig {
    private final FailureRepository failureRepository;

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory.getIfAvailable());
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

    private DefaultErrorHandler errorHandler() {
//        var fixedBackOff = new FixedBackOff(1000L, 2); // 1초 간격으로 2번 더 시도한다. (총 3번)

        // 재시도 간격을 다르게 설정할 수 있다.
        ExponentialBackOffWithMaxRetries expBackOff = new ExponentialBackOffWithMaxRetries(1);  // 최대 재시도 횟수
        expBackOff.setInitialInterval(1_000L);                                 // 초기 간격 : 1초
        expBackOff.setMultiplier(2.0);                                         // 간격 2배씩 증가. 1초 -> 2초 -> 4초
        expBackOff.setMaxInterval(2_000L);                                     // 최대 간격 : 2초

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(consumerRecordRecoverer(), expBackOff);

        errorHandler.setRetryListeners(retryListener());

        // Retry 하지 않을 예외 설정
//        List<Class<? extends Exception>> notRetryableExceptions = List.of(IllegalArgumentException.class);
//        notRetryableExceptions.forEach(errorHandler::addNotRetryableExceptions);

        // Retry 할 예외 설정
//        var exceptionToRetryList = List.of(RecoverableDataAccessException.class);
//        exceptionToRetryList.forEach(errorHandler::addRetryableExceptions);

        return errorHandler;
    }

    private RetryListener retryListener() {
        return (record, ex, deliveryAttempt) ->
                log.info("Failed Record in Retry Listener, Exception = {}, deliveryAttempt = {}", ex.getMessage(), deliveryAttempt);
    }

    private ConsumerRecordRecoverer consumerRecordRecoverer() {
        return (consumerRecord, e) -> {
            var record = (ConsumerRecord<String, String>) consumerRecord;
            failureRepository.save(new FailureEntity(null, record.value()));
        };
    }
}
