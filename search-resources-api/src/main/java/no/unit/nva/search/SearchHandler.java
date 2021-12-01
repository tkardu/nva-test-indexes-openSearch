package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import no.unit.nva.search.models.Query;
import nva.commons.apigateway.ApiGatewayHandler;
import nva.commons.apigateway.RequestInfo;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.search.SearchResponse;

import static no.unit.nva.search.RequestUtil.toQuery;
import static no.unit.nva.search.SearchClientConfig.defaultSearchClient;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static nva.commons.core.attempt.Try.attempt;

public class SearchHandler extends ApiGatewayHandler<Void, String> {

    private final SearchClient searchClient;

    @JacocoGenerated
    public SearchHandler() {
        this(new Environment(), defaultSearchClient());
    }

    public SearchHandler(Environment environment, SearchClient searchClient) {
        super(Void.class, environment, objectMapperWithEmpty);
        this.searchClient = searchClient;
    }

    @Override
    protected String processInput(Void input, RequestInfo requestInfo, Context context) throws ApiGatewayException {
        String indexName = getIndexName(requestInfo);
        Query query = toQuery(requestInfo);

        SearchResponse searchResponse = searchClient.doSearch(query, indexName);
        return searchResponse.toString();
    }

    private String getIndexName(RequestInfo requestInfo) {
        String indexName = attempt(() -> requestInfo.getPathParameter("index")).orElseThrow();
        return indexName;
    }

    @Override
    protected Integer getSuccessStatusCode(Void input, String output) {
        return HttpStatus.SC_OK;
    }
}
