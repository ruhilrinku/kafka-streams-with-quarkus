package org.brightly.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@ApplicationScoped
public class MyKafkaProducer {

    private final KafkaProducer<String, String> producer;

    @Inject
    public MyKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:5402");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "Events-Producer-" + System.currentTimeMillis());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    public KafkaProducer<String, String> getProducer() {
        return this.producer;
    }
}
