package com.fastcampus.kafkapractice;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeyEx {
    private static final Logger log = LoggerFactory.getLogger(ProducerWithKeyEx.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException{
        // 1. property 세팅
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:29092");// 커넥션을 위한 세팅
        properties.setProperty("key.serializer", StringSerializer.class.getName());// key를 직렬화 하기 위한 세팅
        properties.setProperty("value.serializer", StringSerializer.class.getName());// value를 직렬화 하기 위한 세팅


        //2. producer 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        //3. recode를 producer에 전송


        for (int i = 0; i < 10; i++) {
            String key = "key" + i;
            String value = "fastcampus" + i;
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java3",key , value);
            producer.send(producerRecord, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) { // 예외시 null
                                log.error(e.getMessage(), e);
                            } else {
                                log.info("key: " + key + "|partition: " + recordMetadata.partition() + "|offset: " + recordMetadata.offset());

                            }

                        }
                    });
        }





        // 4. producer 종료
        producer.close();


    }
}
