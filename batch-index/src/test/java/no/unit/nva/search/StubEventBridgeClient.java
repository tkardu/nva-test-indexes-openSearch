package no.unit.nva.search;

import nva.commons.core.SingletonCollector;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;

import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static nva.commons.core.attempt.Try.attempt;

public class StubEventBridgeClient implements EventBridgeClient {

    private ImportDataRequestEvent latestEvent;

    public ImportDataRequestEvent getLatestEvent() {
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

    private ImportDataRequestEvent saveContainedEvent(PutEventsRequest putEventsRequest) {
        PutEventsRequestEntry eventEntry = putEventsRequest.entries()
            .stream()
            .collect(SingletonCollector.collect());
        return attempt(eventEntry::detail)
            .map(jsonString -> objectMapperWithEmpty.readValue(jsonString, ImportDataRequestEvent.class))
            .orElseThrow();
    }
}
