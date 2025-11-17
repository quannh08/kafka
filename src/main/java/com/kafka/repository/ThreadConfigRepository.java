package com.kafka.repository;

import com.kafka.entity.ThreadConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ThreadConfigRepository extends JpaRepository<ThreadConfig, String> {
    Optional<ThreadConfig> findByConfigKey(String s);
}
