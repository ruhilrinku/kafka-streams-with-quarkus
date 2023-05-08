package org.brightly.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

@ApplicationScoped
public class KStoreReader {

    @Inject
    KafkaStreams kafkaStreams;

    public KeyValueIterator<String, String> readKeyStore() {
        ReadOnlyKeyValueStore<String, String> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("orders-table-store",
                        QueryableStoreTypes.keyValueStore()));

        return store.all();
    }
}
