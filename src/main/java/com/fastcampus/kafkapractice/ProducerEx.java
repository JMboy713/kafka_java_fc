package com.fastcampus.kafkapractice;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

public class ProducerEx {

    public static void main(String[] args) {

//        SpringApplication.run(KafkaPracticeApplication.class, args);

        // 1. property 세팅
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:29092");// 커넥션을 위한 세팅
        properties.setProperty("key.serializer", StringSerializer.class.getName());// key를 직렬화 하기 위한 세팅
        properties.setProperty("value.serializer", StringSerializer.class.getName());// value를 직렬화 하기 위한 세팅


        //2. producer 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        //3. recode를 producer에 전송

        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("deom_java", "test message");
        producer.send(producerRecord);


        // 4. producer 종료
        producer.close();
    }

}
