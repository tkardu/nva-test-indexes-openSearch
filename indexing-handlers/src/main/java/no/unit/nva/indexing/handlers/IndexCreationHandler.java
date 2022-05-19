package no.unit.nva.indexing.handlers;

import static nva.commons.core.attempt.Try.attempt;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import no.unit.nva.search.IndexingClient;
import nva.commons.core.JacocoGenerated;

public class IndexCreationHandler implements RequestHandler<Object, String> {

    public static final String RESOURCES = "resources";
    public static final String DOIREQUESTS = "doirequests";
    public static final String MESSAGES = "messages";
    public static final String SUCCESS_RESPONSE = "OK";
    private final IndexingClient indexingClient;

    @JacocoGenerated
    public IndexCreationHandler() {
        this(new IndexingClient());
    }

    public IndexCreationHandler(IndexingClient indexingClient) {
        this.indexingClient = indexingClient;
    }

    @Override
    public String handleRequest(Object input, Context context) {
        var indicesClientWrapper = indexingClient.getIndicesClientWrapper();

        attempt(() -> indexingClient.createIndexBasedOnName(RESOURCES, indicesClientWrapper)).orElseThrow();
        attempt(() -> indexingClient.createIndexBasedOnName(DOIREQUESTS, indicesClientWrapper)).orElseThrow();
        attempt(() -> indexingClient.createIndexBasedOnName(MESSAGES, indicesClientWrapper)).orElseThrow();
        return SUCCESS_RESPONSE;
    }
}
