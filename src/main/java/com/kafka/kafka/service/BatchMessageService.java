package com.kafka.kafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Service
@Slf4j(topic = "BATCH-MESSAGE-SERVICE")
@RequiredArgsConstructor
public class BatchMessageService {

    private static final int BATCH_SIZE = 10;     // đủ 10 message mới xử lý
    private static final long TIMEOUT_MS = 5000; // 10 giây timeout

    @Qualifier("customExecutor")
    private final ThreadPoolTaskExecutor executor;

    private final TransactionService transactionService;

    private final BlockingQueue<String> buffer = new LinkedBlockingQueue<>();

    private long lastBatchTime = System.currentTimeMillis();

    /**
     * Nhận message vào cho vào hàng đợi
     * */
    public void receiveMessage(String message) {
        buffer.add(message);

        // Nếu đủ 10 message thì xử lý ngay
        if (buffer.size() >= BATCH_SIZE) {
            flushBatch("FULL_BATCH");
        }
    }

    /**
     * Định kỳ kiểm tra nếu quá thời gian mà chưa đủ batch -> vẫn xử lý
     */
    @Scheduled(fixedDelay = 2000)
    public void flushIfTimeout() {
        long now = System.currentTimeMillis();
        if (!buffer.isEmpty() && (now - lastBatchTime >= TIMEOUT_MS)) {
            flushBatch("TIMEOUT");
        }
    }

    /**
     * Gửi batch sang thread pool xử lý
     */
    private synchronized void flushBatch(String reason) {
        if (buffer.isEmpty()) return;

        List<String> batch = new ArrayList<>();
        buffer.drainTo(batch, BATCH_SIZE);

        lastBatchTime = System.currentTimeMillis();
        executor.submit(() -> {
            try {
                processBatch(batch, reason);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        log.info("Submit to thread");
    }

    /**
     * Xử lý batch message
     */
    private void processBatch(List<String> batch, String reason) throws InterruptedException {
        log.info(Thread.currentThread().getName() +
                " Processing (" + reason + "): " + batch);;

        for (String message : batch) {
            transactionService.saveTransaction(message);
        }
    }
}
