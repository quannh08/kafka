package com.kafka.kafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class ThreadPoolConfig {

    @Bean(name = "customExecutor")
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        executor.setCorePoolSize(5);        // số luồng tối thiểu
        executor.setMaxPoolSize(10);        // số luồng tối đa
        executor.setQueueCapacity(50);      // số task có thể chờ
        executor.setKeepAliveSeconds(60);
        executor.setThreadNamePrefix("BatchThread-");
        executor.initialize();

        return executor;
    }
}
