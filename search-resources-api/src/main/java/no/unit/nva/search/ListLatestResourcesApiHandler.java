package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.handlers.ApiGatewayHandler;
import nva.commons.handlers.RequestInfo;
import nva.commons.handlers.RestRequestHandler;
import nva.commons.utils.Environment;
import nva.commons.utils.JacocoGenerated;
import org.apache.http.HttpStatus;
import org.slf4j.LoggerFactory;

import static no.unit.nva.search.RequestUtil.getResults;

public class ListLatestResourcesApiHandler extends ApiGatewayHandler<Void, SearchResourcesResponse> {

    public static final String SEARCH_ALL_PUBLICATIONS = "*";
    private static final String DEFAULT_SORTFIELD = "owner";
    private final ElasticSearchHighLevelRestClient elasticSearchClient;

    @JacocoGenerated
    public ListLatestResourcesApiHandler() {
        this(new Environment());
    }

    public ListLatestResourcesApiHandler(Environment environment) {
        this(environment, new ElasticSearchHighLevelRestClient(environment));
    }

    public ListLatestResourcesApiHandler(Environment environment,
                                         ElasticSearchHighLevelRestClient elasticSearchClient) {
        super(Void.class, environment, LoggerFactory.getLogger(ListLatestResourcesApiHandler.class));
        this.elasticSearchClient = elasticSearchClient;
    }


    /**
     * Implements the main logic of the handler. Any exception thrown by this method will be handled by {@link
     * RestRequestHandler#handleExpectedException} method.
     *
     * @param input       The input object to the method. Usually a deserialized json.
     * @param requestInfo Request headers and path.
     * @param context     the ApiGateway context.
     * @return the Response body that is going to be serialized in json
     * @throws ApiGatewayException all exceptions are caught by writeFailure and mapped to error codes through the
     *                             method {@link RestRequestHandler#getFailureStatusCode}
     */
    @Override
    protected SearchResourcesResponse processInput(Void input,
                                                   RequestInfo requestInfo,
                                                   Context context) throws ApiGatewayException {
        int results = getResults(requestInfo);
        return elasticSearchClient.searchSingleTermSortedResult(SEARCH_ALL_PUBLICATIONS, results, DEFAULT_SORTFIELD);
    }

    /**
     * Define the success status code.
     *
     * @param input  The request input.
     * @param output The response output
     * @return the success status code.
     */
    @Override
    protected Integer getSuccessStatusCode(Void input, SearchResourcesResponse output) {
        return HttpStatus.SC_OK;
    }
}
