package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import no.unit.nva.search.models.SearchResourcesResponse;
import nva.commons.apigateway.ApiGatewayHandler;
import nva.commons.apigateway.RequestInfo;
import nva.commons.apigateway.RestRequestHandler;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import org.apache.http.HttpStatus;

import static no.unit.nva.search.RequestUtil.toQuery;
import static no.unit.nva.search.SearchClientConfig.defaultSearchClient;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_ENDPOINT_INDEX;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;

public class SearchResourcesApiHandler extends ApiGatewayHandler<Void, SearchResourcesResponse> {

    private final SearchClient elasticSearchClient;

    @JacocoGenerated
    public SearchResourcesApiHandler() {
        this(new Environment(), defaultSearchClient());
    }

    public SearchResourcesApiHandler(Environment environment, SearchClient elasticSearchClient) {
        super(Void.class, environment, objectMapperWithEmpty);
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
        var query = toQuery(requestInfo);
        return elasticSearchClient.searchSingleTerm(query, ELASTICSEARCH_ENDPOINT_INDEX);
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
