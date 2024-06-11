package com.fastcampus.kafkapractice;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerGracefulShutdown {
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

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Starting exit...");
            // wakeup : 정상적인 종료를 위해 사용
            // record polling 중에 어떤 장애가 발생하더라도, 데이터 유실을 막기 위한 안전한 예외 처리
            consumer.wakeup();

            try {
                mainThread.join();// 메인 스레드가 종료하느 것을 기다림
            } catch (InterruptedException e) {
                log.error("Error while waiting for main thread", e);
            }
        }));

        try {


            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> records = consumer.poll(1000); // polling 주기
                for (ConsumerRecord<String, String> record : records) {
                    log.info("key: " + record.key() + "|value: " + record.value());
                    log.info("partition: " + record.partition() + "|offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.error("WakeupException", e);
        }catch (Exception e) {
            log.error("Exception", e);
        }finally {
            consumer.close();
            log.info("Closed consumer and we are done");
        }
    }
}
