package com.quan.kafkademo.exp;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerFastStart {
    public static final String brokerList = "192.168.235.129:9092,192.168.235.130:9092,192.168.235.131:9092";
    public static final String topic = "topic-demo";

    public static void main(String[] args) {
        Properties properties = initConfig();
        stringSend(properties);
//        companySend(properties);
    }


    public static void stringSend(Properties properties) {
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer(properties);
        ProducerRecord<String,String> record = new ProducerRecord<>(topic,"hello,kafka");
        send(kafkaProducer,record);

    }

    public static void companySend(Properties properties) {
        KafkaProducer<String,Company> kafkaProducer = new KafkaProducer(properties);
        Company company = Company.builder().name("hiddenkafka").address("China").build();
        ProducerRecord<String,Company> record = new ProducerRecord<>(topic,company);
        try{
            kafkaProducer.send(record);
        }catch(Exception e) {
            e.printStackTrace();
        }finally{
            kafkaProducer.close();
        }
    }

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,CompanySeralizer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"client.id.demo");
//        properties.put(ProducerConfig.RETRIES_CONFIG,10);
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,ProducerInterceptorPrefix.class.getName()+
                ","+ProducerInterceptorPrefixPlus.class.getName());
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,DemoPartitioner.class.getName());
        return properties;
    }

    public static void send(KafkaProducer<String,String> kafkaProducer,ProducerRecord<String,String> record){
        try{
            int i = 0;
            while (i<10){
                i++;
              kafkaProducer.send(record);
            }
        }catch(Exception e) {
            e.printStackTrace();
        }finally{
            kafkaProducer.close();
        }
    }

    public static void syncSend(KafkaProducer<String,String> kafkaProducer,ProducerRecord<String,String> record){
        try{
            RecordMetadata recordMetadata = kafkaProducer.send(record).get();
            System.out.println("11111111"+recordMetadata.topic()+"-"+recordMetadata.partition()+"-"+recordMetadata.offset());
        }catch(ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }finally{
            kafkaProducer.close();
        }
    }

    public static void asyncSend(KafkaProducer<String,String> kafkaProducer,ProducerRecord<String,String> record){
        try{
            kafkaProducer.send(record, (metadata,exception)->
            {
                if(exception!=null){
                    exception.printStackTrace();
                }else{
                    System.out.println("11111111"+metadata.topic()+"-"+metadata.partition()+"-"+metadata.offset());
                }
            });
        }catch(Exception e) {
            e.printStackTrace();
        }finally{
            kafkaProducer.close();
        }
    }
}
