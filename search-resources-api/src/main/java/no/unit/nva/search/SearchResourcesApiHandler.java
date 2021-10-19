package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import nva.commons.apigateway.ApiGatewayHandler;
import nva.commons.apigateway.RequestInfo;
import nva.commons.apigateway.RestRequestHandler;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.JsonUtils;
import org.apache.http.HttpStatus;
import org.elasticsearch.search.sort.SortOrder;

import static no.unit.nva.search.RequestUtil.getFrom;
import static no.unit.nva.search.RequestUtil.getOrderBy;
import static no.unit.nva.search.RequestUtil.getResults;
import static no.unit.nva.search.RequestUtil.getSearchTerm;
import static no.unit.nva.search.RequestUtil.getSortOrder;

public class SearchResourcesApiHandler extends ApiGatewayHandler<Void, SearchResourcesResponse> {

    private final ElasticSearchHighLevelRestClient elasticSearchClient;

    @JacocoGenerated
    public SearchResourcesApiHandler() {
        this(new Environment());
    }

    public SearchResourcesApiHandler(Environment environment) {
        this(environment, new ElasticSearchHighLevelRestClient());
    }

    public SearchResourcesApiHandler(Environment environment, ElasticSearchHighLevelRestClient elasticSearchClient) {
        super(Void.class, environment, JsonUtils.objectMapperWithEmpty);
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
        String searchTerm = getSearchTerm(requestInfo);
        int results = getResults(requestInfo);
        int from = getFrom(requestInfo);
        String orderBy = getOrderBy(requestInfo);
        SortOrder sortOrder = getSortOrder(requestInfo);
        return elasticSearchClient.searchSingleTerm(searchTerm, results, from, orderBy, sortOrder);
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
