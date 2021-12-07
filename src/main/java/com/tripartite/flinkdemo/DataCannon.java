package com.tripartite.flinkdemo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class DataCannon {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "flink-kafka-demo-data-generaor");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        KafkaProducer<String, Integer> kafkaProducer = new KafkaProducer<>(properties);
        try {
            for (int i = 0; i < 1; i++) {
//                input1(kafkaProducer);
                input2(kafkaProducer);
            }
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    private static void input2(KafkaProducer<String, Integer> kafkaProducer) throws ExecutionException, InterruptedException {
        String[] fks = new String[] {"random-1", "random-2", "random-3", "random-4", "random-5", "random-6"};
        Random random = new Random();
//        for (int i = 0; i < 6; i++) {
//            ProducerRecord record = new ProducerRecord<>("flink.in.2", String.format("%s", fks[random.nextInt(fks.length)]), i);
            ProducerRecord record = new ProducerRecord<>("flink.in.2", String.format("random-0"), 15);
            kafkaProducer.send(record);
//        }
    }

    private static void input1(KafkaProducer<String, Integer> kafkaProducer) throws InterruptedException, ExecutionException {
        String[] fks = new String[] {"random-1", "random-2", "random-3", "random-4", "random-5", "random-6"};
        Random random = new Random();
//        for (int i = 0; i < 10; i++) {
//            ProducerRecord record = new ProducerRecord<>("flink.in.1", String.format("test-%d:%s", i, fks[random.nextInt(fks.length)]), i);
            ProducerRecord record = new ProducerRecord<>("flink.in.1", String.format("test-0:random-0"), 0);
            kafkaProducer.send(record);
//        }
    }

}
/*
test-1:random1 -> 10         random1 - 40
test-2:random2 -> 15         random5 - 50
test-3:random5
test-3:random2
test-4:random5
test-6:random5
...
test-10:random1 - 100
test-10:random1 - 90

output: test-1:random1, lhs(test-1:random1, 10), (random1, 40)
output: test-10:random1, lhs(test-10:random1, 100), (random1, 40)
output: test-10:random1, lhs(test-10:random1, 90), (random1, 40)
 */