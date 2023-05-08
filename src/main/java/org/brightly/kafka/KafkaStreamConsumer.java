package org.brightly.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.StreamsBuilder;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.JoinWindows;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;

@ApplicationScoped
public class KafkaStreamConsumer {

    @ConfigProperty(name = "platform.kafka-topic")
    String topicName = "orders";

    @Produces
    public Topology buildTopology() {

        //Create Topology
        StreamsBuilder builder = new StreamsBuilder();

        //Consume events from a topic via K-Stream
        KStream<String, String> sampleKStream = builder.stream(
                "orders-in",
                Consumed.with(Serdes.String(), Serdes.String()));

        sampleKStream.foreach((key, value) -> {
            System.out.println("Before Processing.......");
            System.out.println("Key: "+key);
            System.out.println("Value: "+value);
        });

        //Aggregation Example
        sampleKStream.groupByKey()
                .aggregate(() -> "",
                        (key, value, aggregate) -> aggregate + value,
                        Materialized.with(Serdes.String(), Serdes.String()))
                .toStream().peek((key, value) -> {
                    System.out.println("Aggregation .......");
                    System.out.println("Key: "+key);
                    System.out.println("Value: "+value);
                });

        //Produce events to topic
        sampleKStream
                .mapValues((readOnlyKey, value) -> value.toUpperCase())
                .peek((key, value) -> {
                    System.out.println("After Processing.......");
                    System.out.println("Key: "+key);
                    System.out.println("Value: "+value);
                })
                .to("orders-out", Produced.with(Serdes.String(), Serdes.String()));

        //Create K-Table and State Store from K-Stream
        sampleKStream.toTable(Named.as("orders-table"),
                Materialized.as("orders-table-store"));

        //Joining Example
        KStream<String, String> studentsKStream = builder.stream(
                "students",
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> addressesKStream = builder.stream(
                "addresses",
                Consumed.with(Serdes.String(), Serdes.String()));

        studentsKStream
                .join(addressesKStream, (readOnlyKey, value1, value2) ->
                        "Student name: " + value1 + "; address: " + value2,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5)))
                .peek((key, value) -> {
                    System.out.println("Students Joined Stream.......");
                    System.out.println("Key: "+key);
                    System.out.println("Value: "+value);
                });

        // Build Topology
        return builder.build();
    }
}
