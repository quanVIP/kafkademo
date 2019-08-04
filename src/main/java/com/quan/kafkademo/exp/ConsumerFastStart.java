package com.quan.kafkademo.exp;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerFastStart {
    public static final String brokerList = "192.168.235.129:9092,192.168.235.130:9092,192.168.235.131:9092";
    public static final String topic= "topic-demo";
    public static final String groupId = "group.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers",brokerList);
//        properties.put("value.deserializer",CompanySeralizer.class.getName());
        properties.put("group.id", groupId);
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        while (true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record: records){
                System.out.println(record.value());
            }
        }
    }

}
