package no.unit.nva.search;

import no.unit.nva.search.models.Query;
import no.unit.nva.search.models.SearchResourcesResponse;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.apigateway.exceptions.BadGatewayException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;

import static no.unit.nva.search.models.SearchResourcesResponse.toSearchResourcesResponse;

public class SearchClient {


    public static final int BULK_SIZE = 100;
    public static final String NO_RESPONSE_FROM_INDEX = "No response from index";
    private final RestHighLevelClientWrapper elasticSearchClient;

    /**
     * Creates a new ElasticSearchRestClient.
     *
     * @param elasticSearchClient client to use for access to ElasticSearch
     */
    public SearchClient(RestHighLevelClientWrapper elasticSearchClient) {
        this.elasticSearchClient = elasticSearchClient;
    }

    /**
     * Searches for a searchTerm or index:searchTerm in elasticsearch index.
     *
     * @param query query object
     * @throws ApiGatewayException thrown when uri is misconfigured, service i not available or interrupted
     */
    public SearchResourcesResponse searchSingleTerm(Query query, String index)
            throws ApiGatewayException {
        var searchResponse = doSearch(query, index);
        return toSearchResourcesResponse(query.getSearchTerm(), searchResponse.toString());
    }

    private SearchResponse doSearch(Query query, String index) throws BadGatewayException {
        try {
            var searchRequest = query.toSearchRequest(index);
            return elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new BadGatewayException(NO_RESPONSE_FROM_INDEX);
        }
    }
}
