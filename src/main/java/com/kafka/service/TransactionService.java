package com.kafka.service;

import com.google.gson.Gson;
import com.kafka.dto.request.TransactionRequest;
import com.kafka.entity.Transaction;
import com.kafka.repository.TransactionRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@Slf4j(topic = "TRANSACTION-SERVICE")
@RequiredArgsConstructor
public class TransactionService {
    private final TransactionRepository transactionRepository;

    @Transactional(rollbackOn =  Exception.class)
    public Long saveTransaction(String message) {
        log.info("save transaction");
        TransactionRequest request = new Gson().fromJson(message, TransactionRequest.class);

        Transaction transaction = Transaction.builder()
                .id(request.getId())
                .userId(request.getUserId())
                .amount(request.getAmount())
                .timestamp(LocalDateTime.now())
                .build();

        if (transactionRepository.existsById(transaction.getId())) {
            log.warn("Duplicate message ignored: {}", transaction.getId());
            return 0L;
        }
        transactionRepository.save(transaction);

        return  transaction.getId();
    }
}
