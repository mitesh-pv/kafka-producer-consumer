package com.lld.app.kafka.output;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerOutputCooperativeRebalance {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerOutputCooperativeRebalance.class.getSimpleName());
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
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        // Create Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // Get reference to current thread
        final Thread mainThread = Thread.currentThread();

        // Add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, lets exit by calling consumer.wakeup() ..");
            kafkaConsumer.wakeup();

            // join the main thread to allow execution of code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException exp) {
                exp.printStackTrace();
            } finally {
                log.info("Print Last");
            }
        }));

        try {

            // Subscribe consumer to our topic
            kafkaConsumer.subscribe(Collections.singletonList(topic));

            // Poll for new data
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> consumerRecord : records) {
                    log.info("Key: " + consumerRecord.key() + ", Value: " + consumerRecord.value());
                    log.info("Partition: " + consumerRecord.partition() + ", Offsets : " + consumerRecord.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Wakeup exception ");
            // ignore this exception as this is expected exception when we are closing the consumer
        } catch (Exception exp) {
            log.error("Unexpected excepion : ", exp);
        } finally {
            kafkaConsumer.close(); // This will also commit the offsets if needed
            log.info("Consumer is now gracefully closed");
        }
    }
}
