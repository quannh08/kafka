package com.kafka.kafka.entity;

import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDateTime;

@Entity
@Data
@Table(name = "thread_config")
public class ThreadConfig {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String configKey;
    private String configValue;
    private LocalDateTime lastUpdated;
}
