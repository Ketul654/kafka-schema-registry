package com.ketul.kafka.producer;

import com.ketul.kafka.avro.schema.Employee;
import com.ketul.kafka.utils.KafkaConstants;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

/*
Producer with schema version 1
 */
public class KafkaAvroProducerV1 {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroProducerV1.class);
    public static void main(String[] args) {
        Properties properties = new Properties();
        // Mandatory properties for producer
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(KafkaConstants.SCHEMA_REGISTRY_URL_CONFIG, KafkaConstants.SCHEMA_REGISTRY_URL_VALUE);

        KafkaProducer<String, Employee> kafkaProducer = new KafkaProducer(properties);

        Employee employee = Employee.newBuilder()
                .setEmployeeId("emp-1")
                .setFistName("Vipul")
                .setLastName("Patel")
                .setAge(30)
                .setSalary(100000f)
                .setIsPermanent(false)
                .setPhoneNumber("123-456-789").build();
        try {
            ProducerRecord record = new ProducerRecord(KafkaConstants.TOPIC_NAME, employee.getEmployeeId(), employee);
            kafkaProducer.send(record, (recordMetadata, e) -> {
                if(e==null) {
                    LOGGER.info("Version 1 record has been sent on {}", recordMetadata.toString());
                } else {
                    LOGGER.error("Exception has occurred while sending employee record to kafka : ", e);
                }
            });
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while producing message : ", ex);
        } finally {
            kafkaProducer.close();
        }
    }
}
