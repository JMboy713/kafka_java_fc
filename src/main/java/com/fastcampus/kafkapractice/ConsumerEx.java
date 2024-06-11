package com.fastcampus.kafkapractice;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerEx {
    private static final Logger log = LoggerFactory.getLogger(ConsumerEx.class.getSimpleName());

    public static void main(String[] args) {
        String topic = "demo_java";
        String groupId = "my-java-application";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:29092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "latest"); // 가장 최근에 생성된 메시지부터 읽어옴

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        while(true){
            log.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(1000); // polling 주기
            for (ConsumerRecord<String, String> record : records) {
                log.info("key: " + record.key() + "|value: " + record.value());
                log.info("partition: " + record.partition() + "|offset: " + record.offset());
            }
        }
    }
}
