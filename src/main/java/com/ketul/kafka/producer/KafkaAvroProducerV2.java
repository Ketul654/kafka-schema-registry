package com.ketul.kafka.producer;

import com.ketul.kafka.avro.schema.EmployeeV1;
import com.ketul.kafka.avro.schema.EmployeeV2;
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
 Producer with backward compatible schema
 */
public class KafkaAvroProducerV2 {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroProducerV2.class);
    public static void main(String[] args) {
        Properties properties = new Properties();
        // Mandatory properties for producer
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url", "http://127.0.0.1:8081");

        KafkaProducer<String, EmployeeV1> kafkaProducer = new KafkaProducer(properties);

        /*
         V2 is backward compatible.
         Make sure schema is configured for backward compatibility on control center.
         This will throw schema compatibility error otherwise if V1 is already registered.
         */
        EmployeeV2 employeeV2 = EmployeeV2.newBuilder()
                .setEmployeeId("emp-2")
                .setFistName("Ketul")
                .setLastName("Patel")
                .setAge(30)
                .setSalary(100000f)
                .setIsPermanent(true).build();
        try {
            ProducerRecord record = new ProducerRecord(KafkaConstants.TOPIC_NAME, employeeV2.getEmployeeId(), employeeV2);
            kafkaProducer.send(record, (recordMetadata, e) -> {
                if(e==null) {
                    LOGGER.info("Version 2 record has been sent on {}", recordMetadata.toString());
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
