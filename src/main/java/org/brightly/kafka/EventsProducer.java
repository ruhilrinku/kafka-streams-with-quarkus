package org.brightly.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

@ApplicationScoped
public class EventsProducer {

    Map<String, String> studentEvents = Map.of(
            "S01", "SAM",
            "S02", "ADAM",
            "S03", "RAMAN");

    Map<String, String> addressEvents = Map.of(
            "S01", "USA",
            "S02", "UK",
            "S03", "INDIA");

    @Inject
    MyKafkaProducer eventsProducer;

    public void produceStudentEvents() {
        System.out.println("Produce Student Events");
        studentEvents.forEach((key, value) -> {
            ProducerRecord<String, String> record = new ProducerRecord<>("students", key, value);
            eventsProducer.getProducer().send(record);
        });
    }

    public void produceAddressEvents() {
        System.out.println("Produce Address Events");
        addressEvents.forEach((key, value) -> {
            ProducerRecord<String, String> record = new ProducerRecord<>("addresses", key, value);
            eventsProducer.getProducer().send(record);
        });
    }

}
