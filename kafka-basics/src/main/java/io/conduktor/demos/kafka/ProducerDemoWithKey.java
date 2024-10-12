package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKey {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKey.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am a Kafka producer with key!");

        String bootstrapServers = "localhost:9092";

        String topicName = "java_demo";

        // create producer properties
        Properties properties = new Properties();

        // connect to localhost
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // set producer properties
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int j = 0; j < 2; j++) {

            for (int i = 0; i < 10; i++) {
                String key = "key" + " " + i;
                String value = "Hello World" + " " + i;

                // create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);

                // send data
                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes everytime a record successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was sent successfully
                            log.info("Topic: {} Partition: {} Offset: {} Timestamp: {} Key: {} Value: {}",
                                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp(), key, value);
                        } else {
                            log.error("Error while producing " + e);
                        }
                    }
                });
            }
        }

        // tell the producer to send all data and block until done -- synchronous
        kafkaProducer.flush();

        // flush and close the producer
        kafkaProducer.close();

    }

}
