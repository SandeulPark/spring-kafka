package org.example.springkafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@RequiredArgsConstructor
@Service
public class MyService {
    private final MyRepository myRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public void save(ConsumerRecord<String, String> record) {
        MyEntity entity = readValue(record);
        if (entity.isFailure()) throw new RuntimeException();
        myRepository.save(entity);
    }

    private MyEntity readValue(ConsumerRecord<String, String> record) {
        try {
            return objectMapper.readValue(record.value(), MyEntity.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
