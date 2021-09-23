package no.unit.nva.search;

import static nva.commons.core.attempt.Try.attempt;
import nva.commons.core.JsonUtils;
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
        this.latestEvent = putEventsRequest.entries()
            .stream()
            .findFirst()
            .map(PutEventsRequestEntry::detail)
            .map(attempt(jsonString -> JsonUtils.objectMapperWithEmpty.readValue(jsonString, ImportDataRequest.class)))
            .orElseThrow()
            .orElseThrow();
        return PutEventsResponse.builder().failedEntryCount(0).build();
    }

    @Override
    public String serviceName() {
        return null;
    }

    @Override
    public void close() {

    }
}
