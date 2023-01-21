package com.lld.app.kafka.output;

import com.google.gson.JsonParser;
import com.lld.app.opensearch.OpenSearchClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    
    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) {
        // Create OpenSearch client
        OpenSearchClient openSearchClient = new OpenSearchClient();
        RestHighLevelClient restHighLevelClient = openSearchClient.createOpenSearchClient();

        // Create kafka client
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        // We need to create an index in openSearch if it doesn't exist
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
        try (restHighLevelClient; kafkaConsumer) {
            boolean indexExists = restHighLevelClient.indices()
                    .exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!indexExists) {
                restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("\'wikimedia\' index created");
            } else {
                log.info("\'wikimedia\' index already exists");
            }

            String topic = "wikimedia.recentchange";
            // Subscribe the consumer to the kafka topic
            kafkaConsumer.subscribe(Collections.singleton(topic));

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received record_count: " + recordCount + " records");

                for (ConsumerRecord<String, String> record: records) {
                    try {
                        // Send the records to OpenSearch

                        // To make the consumer idempotent
                        // Method 1: Define id using kafka records coordinates
                        // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                        // Method 2: Extract id from the record
                        String id = extractId(record.value());
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
                        log.info("Inserted one document into OpenSearch with _id : " + indexResponse.getId());
                    } catch (Exception exp) {
                        log.error(String.valueOf(exp));
                    }
                }
                // TODO: Commit offsets after the batch is consumed - when ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG is false
                // kafkaConsumer.commitSync();
                // log.info("Offsets committed");
            }
        } catch (IOException e) {
            log.error(String.valueOf(e));
        }

        // Main code logic

        // Close things
    }

    private static String extractId(String value) {
        return JsonParser.parseString(value)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "consumer-opensearch-demo";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // auto.offset.reset -> none/earliest/latest
        // none -> if no previous offsets are found, then don't start
        // earliest -> read from the beginning of the topic
        // latest -> read from the tail
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // Disable auto offsets commit
        // properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(properties);
    }
}
