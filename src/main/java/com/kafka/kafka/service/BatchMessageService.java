package com.kafka.kafka.service;

import com.kafka.kafka.record.MessageWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j(topic = "BATCH-MESSAGE-SERVICE")
@RequiredArgsConstructor
public class BatchMessageService {

    private static final int BATCH_SIZE = 10;     // đủ 10 message mới xử lý
    private static final long TIMEOUT_MS = 5000; // 5 giây timeout

    private final AtomicLong totalDrainedMessages = new AtomicLong(0);

    @Qualifier("customExecutor")
    private final ThreadPoolTaskExecutor executor;

    private final TransactionService transactionService;

    private final RetryHandlerService retryHandlerService;

    private final BlockingQueue<MessageWrapper> buffer = new LinkedBlockingQueue<>();

    private long lastBatchTime = System.currentTimeMillis();

    /**
     * Nhận message vào cho vào hàng đợi
     * */
    public void receiveMessage(String message, Acknowledgment ack,  Headers headers) {
        buffer.add(new MessageWrapper(message, ack, headers));

        // Nếu đủ 10 message thì xử lý ngay
        if (buffer.size() >= BATCH_SIZE) {
            flushBatch("FULL_BATCH");
        }
    }

    /**
     * Định kỳ kiểm tra nếu quá thời gian mà chưa đủ batch -> vẫn xử lý
     */
    @Scheduled(fixedDelay = 1000)
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

        List<MessageWrapper> batch = new ArrayList<>();
        buffer.drainTo(batch, BATCH_SIZE);

//        int drained = buffer.drainTo(batch, BATCH_SIZE);
//        totalDrainedMessages.addAndGet(drained);

        lastBatchTime = System.currentTimeMillis();
        executor.submit(() -> {
            try {
                processBatch(batch, reason);
                batch.forEach(msg -> msg.ack().acknowledge());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        log.info("Submit to thread");

    }

    /**
     * Xử lý batch message
     */
    private void processBatch(List<MessageWrapper> batch, String reason) throws InterruptedException {
        log.info(Thread.currentThread().getName() +
                " Processing (" + reason + "): " + batch);;

        for (MessageWrapper msg : batch) {
            try {
                transactionService.saveTransaction(msg.message());
            } catch (Exception e) {
                int retryCount = retryHandlerService.getRetryCount(msg.headers());
                log.error("Failed to save message (retryCount={}): {}", retryCount, e.getMessage());
                retryHandlerService.handleError(msg.message(), retryCount);
                continue;
            }

            //ack khi xử lý thành công
            msg.ack().acknowledge();
        }
    }
}
