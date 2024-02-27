package org.example.springkafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class MyConsumer {
    private final MyService myService;

    @KafkaListener(topics = "my-topic", groupId = "my-group-id")
    public void onMessage(ConsumerRecord<String, String> record) {
        log.info("record = {}", record);
        myService.save(record);
    }
}
