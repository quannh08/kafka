package com.kafka.kafka.config;

import com.kafka.kafka.repository.ThreadConfigRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class ThreadPoolConfig {

    @Autowired
    private ThreadConfigRepository threadConfigRepository;

    private ThreadPoolTaskExecutor executor;

    @Bean(name = "customExecutor")
    public ThreadPoolTaskExecutor taskExecutor() {

        executor = new ThreadPoolTaskExecutor();
        int poolSize = getThreadPoolSizeFromDb();

        executor.setCorePoolSize(poolSize);        // số luồng tối thiểu
        executor.setMaxPoolSize(poolSize*2);        // số luồng tối đa
        executor.setQueueCapacity(50);          // số task có thể chờ
        executor.setKeepAliveSeconds(30);
        executor.setAllowCoreThreadTimeOut(true);
        executor.setThreadNamePrefix("BatchThread-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();

        return executor;
    }

    // Đọc số lượng luồng từ DB
    private int getThreadPoolSizeFromDb() {
        return threadConfigRepository.findByConfigKey("thread.pool.size")
                .map(cfg -> Integer.parseInt(cfg.getConfigValue()))
                .orElse(5); // mặc định 5 nếu chưa có trong DB
    }

    @Scheduled(fixedRate = 60000)
    public void refreshThreadPool() {
        int newSize = getThreadPoolSizeFromDb();
        if (executor != null && newSize != executor.getCorePoolSize()) {
            System.out.println("Updating thread pool size from "
                    + executor.getCorePoolSize() + " → " + newSize);
            executor.setCorePoolSize(newSize);
            executor.setMaxPoolSize(newSize*2);
        }
    }
}
