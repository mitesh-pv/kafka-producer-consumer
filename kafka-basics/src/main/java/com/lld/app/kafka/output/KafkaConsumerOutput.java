package com.lld.app.kafka.output;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerOutput {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerOutput.class.getSimpleName());
    public static void main(String[] args) {

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-second-application";
        String topic = "demo_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // auto.offset.reset -> none/earliest/latest
        // none -> if no previous offsets are found, then don't start
        // earliest -> read from the beginning of the topic
        // latest -> read from the tail
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // Subscribe consumer to our topic
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        // Poll for new data
        while (true) {
            log.info("Polling");
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> consumerRecord : records) {
                log.info("Key: " + consumerRecord.key() + ", Value: " + consumerRecord.value());
                log.info("Partition: " + consumerRecord.partition() + ", Offsets : " + consumerRecord.offset());
            }
        }

    }
}
