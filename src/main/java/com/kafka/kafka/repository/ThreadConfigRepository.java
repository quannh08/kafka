package com.kafka.kafka.repository;

import aj.org.objectweb.asm.commons.Remapper;
import com.kafka.kafka.entity.ThreadConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ThreadConfigRepository extends JpaRepository<ThreadConfig, String> {
    Optional<ThreadConfig> findByConfigKey(String s);
}
