package org.brightly;

import com.google.protobuf.Empty;
import io.quarkus.grpc.GrpcService;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.brightly.kafka.EventsProducer;
import org.brightly.kafka.KStoreReader;

@GrpcService
public class KStoreGrpcService implements KStoreGrpc {

    @Inject
    KStoreReader kStoreReader;

    @Inject
    EventsProducer eventsProducer;

    @Override
    public Uni<MessageResponse> readAll(Empty request) {
        KeyValueIterator<String, String> keyValueIterator = kStoreReader.readKeyStore();

        MessageResponse.Builder messageResponse = MessageResponse.newBuilder();

        while(keyValueIterator.hasNext()) {
            KeyValue<String, String> keyValue = keyValueIterator.next();
            messageResponse.addKeyValues(KeyValueReply.newBuilder()
                    .setKey(keyValue.key)
                    .setValue(keyValue.value)
                    .build());
        }
        return Uni.createFrom().item(messageResponse.build());
    }

    @Override
    public Uni<Empty> produceEvents(Empty request) {
        eventsProducer.produceStudentEvents();
        eventsProducer.produceAddressEvents();
        return Uni.createFrom().nullItem();
    }

}
