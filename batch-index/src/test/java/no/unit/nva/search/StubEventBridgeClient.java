package no.unit.nva.search;

import static nva.commons.core.attempt.Try.attempt;
import nva.commons.core.JsonUtils;
import nva.commons.core.SingletonCollector;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;

public class StubEventBridgeClient implements EventBridgeClient {

    private ImportDataRequest latestEvent;

    public ImportDataRequest getLatestEvent() {
        return latestEvent;
    }

    public PutEventsResponse putEvents(PutEventsRequest putEventsRequest) {
        this.latestEvent = saveContainedEvent(putEventsRequest);
        return PutEventsResponse.builder().failedEntryCount(0).build();
    }

    @Override
    public String serviceName() {
        return null;
    }

    @Override
    public void close() {

    }

    private ImportDataRequest saveContainedEvent(PutEventsRequest putEventsRequest) {
        PutEventsRequestEntry eventEntry = putEventsRequest.entries()
            .stream()
            .collect(SingletonCollector.collect());
        return attempt(eventEntry::detail)
            .map(jsonString -> JsonUtils.objectMapperWithEmpty.readValue(jsonString, ImportDataRequest.class))
            .orElseThrow();
    }
}