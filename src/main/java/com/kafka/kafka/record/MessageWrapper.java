package com.kafka.kafka.record;

import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.support.Acknowledgment;

public record MessageWrapper(String message, Acknowledgment ack, Headers headers) {
}
