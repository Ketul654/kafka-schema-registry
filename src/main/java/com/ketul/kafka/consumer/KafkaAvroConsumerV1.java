package com.ketul.kafka.consumer;

import com.ketul.kafka.avro.schema.v1.Employee;
import com.ketul.kafka.utils.KafkaConstants;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class KafkaAvroConsumerV1 {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroConsumerV1.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
        properties.put("schema.registry.url", "http://127.0.0.1:8081");
        properties.put("specific.avro.reader", true);

        ArrayList<String> topics = new ArrayList<String>();
        topics.add(KafkaConstants.TOPIC_NAME);

        KafkaConsumer<String, Employee> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(topics);

        try {
            while (true) {

                ConsumerRecords<String, Employee> records = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, Employee> record : records) {
                    LOGGER.info(String.format("Topic : %s, Partition : %s, Offset : %s, Key : %s, Value : %s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while consuming employee data : ", ex);
        } finally {
            kafkaConsumer.close();
        }
    }
}
