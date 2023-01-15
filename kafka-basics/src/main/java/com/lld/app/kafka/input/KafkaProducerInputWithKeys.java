package com.lld.app.kafka.input;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerInputWithKeys {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerInputWithKeys.class.getSimpleName());
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
        ProducerRecord<String, String> record = null;

        for (int i=0; i<10; ++i) {
            String topic = "demo_topic";
            String key = "id_" + i;
            String value = "message from java sdk" + i;

            record = new ProducerRecord<>(topic, key, value);
            // Send data - asynchronously
            ProducerRecord<String, String> finalRecord = record;
            kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // This callback method executes everytime a message is sent successfully to kafka or an exception is thrown
                    if (e == null) {
                        log.info("Received new metadata/ \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Key: " + finalRecord.key() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offsets: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        log.error("Error producing : ", e);
                    }
                }
            });
        }

        // Flush and close the producer - flush is synchronous process
        kafkaProducer.flush();
        kafkaProducer.close(); // close by-default calls flush
    }
}
