package com.lihaogn.sparkTest.kafka;

/**
 * kafka 测试
 */
public class KafkaClientApp {
    public static void main(String[] args) {

        new com.lihaogn.sparkTest.kafka.KafkaProducer(com.lihaogn.sparkTest.kafka.KafkaProperties.TOPIC).start();

        new com.lihaogn.sparkTest.kafka.KafkaConsumer(com.lihaogn.sparkTest.kafka.KafkaProperties.TOPIC).start();


    }
}
