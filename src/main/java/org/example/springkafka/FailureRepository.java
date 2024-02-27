package org.example.springkafka;

import org.springframework.data.jpa.repository.JpaRepository;

public interface FailureRepository extends JpaRepository<FailureEntity, Long> {
}
