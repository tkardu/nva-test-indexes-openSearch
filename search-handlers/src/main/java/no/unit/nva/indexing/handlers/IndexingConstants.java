package no.unit.nva.indexing.handlers;

import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import nva.commons.core.JacocoGenerated;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;

public final class IndexingConstants {

    private IndexingConstants() {
    }

    @JacocoGenerated
    public static EventBridgeClient defaultEventBridgeClient() {
        return EventBridgeClient.builder()
                .httpClient(UrlConnectionHttpClient.create())
                .build();
    }

    @JacocoGenerated
    public static ElasticSearchHighLevelRestClient defaultEsClient() {
        return new ElasticSearchHighLevelRestClient();
    }
}
