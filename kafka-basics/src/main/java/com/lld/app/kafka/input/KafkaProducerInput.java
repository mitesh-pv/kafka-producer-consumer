package com.lld.app.kafka.input;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerInput {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerInput.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Kafka Producer");

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // Create a producer records
        ProducerRecord<String, String> record = new ProducerRecord<>("demo_topic", "second message-from-java-sdk");

        // Send data - asynchronously
        kafkaProducer.send(record);

        // Flush and close the producer - flush is synchronous process
        kafkaProducer.flush();
        kafkaProducer.close(); // close by-default calls flush
    }
}
