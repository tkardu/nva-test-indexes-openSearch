package no.unit.nva.indexing.handlers;

import static no.unit.nva.search.constants.ApplicationConstants.DOIREQUESTS_INDEX;
import static no.unit.nva.search.constants.ApplicationConstants.MESSAGES_INDEX;
import static no.unit.nva.search.constants.ApplicationConstants.PUBLISHING_REQUESTS_INDEX;
import static no.unit.nva.search.constants.ApplicationConstants.RESOURCES_INDEX;
import static nva.commons.core.attempt.Try.attempt;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import no.unit.nva.search.IndexingClient;
import nva.commons.core.JacocoGenerated;

public class InitHandler implements RequestHandler<Object, String> {
    
    public static final String SUCCESS_RESPONSE = "OK";
    private final IndexingClient indexingClient;
    
    @JacocoGenerated
    public InitHandler() {
        this(new IndexingClient());
    }
    
    public InitHandler(IndexingClient indexingClient) {
        this.indexingClient = indexingClient;
    }
    
    @Override
    public String handleRequest(Object input, Context context) {
        attempt(() -> indexingClient.createIndex(RESOURCES_INDEX)).orElseThrow();
        attempt(() -> indexingClient.createIndex(DOIREQUESTS_INDEX)).orElseThrow();
        attempt(() -> indexingClient.createIndex(MESSAGES_INDEX)).orElseThrow();
        attempt(() -> indexingClient.createIndex(PUBLISHING_REQUESTS_INDEX)).orElseThrow();
        return SUCCESS_RESPONSE;
    }
}
