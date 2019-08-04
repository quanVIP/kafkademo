package com.quan.kafkademo.exp;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class ProducerInterceptorPrefixPlus implements ProducerInterceptor {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        String modifyValue = "prefix2-"+ record.value();
        return  new ProducerRecord(record.topic(),record.partition(),record.timestamp(),record.key(),modifyValue,record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
