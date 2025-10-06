package com.kafka.kafka.service;

import com.google.gson.Gson;
import com.kafka.kafka.dto.request.TransactionRequest;
import com.kafka.kafka.entity.Transaction;
import com.kafka.kafka.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {
    private final TransactionRepository transactionRepository;

    public Long saveTransaction(String message) {
        log.info("save transaction");
        TransactionRequest request = new Gson().fromJson(message, TransactionRequest.class);

        Transaction transaction = Transaction.builder()
                .amount(request.getAmount())
                .timestamp(LocalDateTime.now())
                .build();

        log.info("save transaction with id {}", transaction.getId());
        transactionRepository.save(transaction);
        return  transaction.getId();
    }
}
