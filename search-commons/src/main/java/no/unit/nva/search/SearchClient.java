package no.unit.nva.search;

import static no.unit.nva.search.models.SearchResourcesResponse.toSearchResourcesResponse;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import java.io.IOException;
import java.net.URI;
import no.unit.nva.search.models.Query;
import no.unit.nva.search.models.SearchResourcesResponse;
import no.unit.nva.search.restclients.responses.ViewingScope;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.apigateway.exceptions.BadGatewayException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class SearchClient {

    public static final String NO_RESPONSE_FROM_INDEX = "No response from index";
    public static final String ORGANIZATION_IDS = "organizationIds";
    public static final String APPROVED = "APPROVED";
    public static final String STATUS = "status";
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
        return toSearchResourcesResponse(query.getRequestUri(), query.getSearchTerm(), searchResponse.toString());
    }

    public SearchResponse findResourcesForOrganizationIds(ViewingScope viewingScope, String... index)
        throws BadGatewayException {
        try {
            SearchRequest searchRequest = createSearchRequestForResourcesWithOrganizationIds(viewingScope, index);
            return elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new BadGatewayException(NO_RESPONSE_FROM_INDEX);
        }
    }

    private SearchRequest createSearchRequestForResourcesWithOrganizationIds(
        ViewingScope viewingScope,
        String... indices) {
        BoolQueryBuilder queryBuilder = matchOneOfOrganizationIdsQuery(viewingScope);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(queryBuilder);
        return new SearchRequest(indices).source(searchSourceBuilder);
    }

    private BoolQueryBuilder matchOneOfOrganizationIdsQuery(ViewingScope viewingScope) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
            .must(existsQuery(ORGANIZATION_IDS));
        for (URI includedOrganizationId : viewingScope.getIncludedUnits()) {
            queryBuilder.must(matchPhraseQuery(ORGANIZATION_IDS, includedOrganizationId.toString()));
        }
        for (URI excludedOrganizationId : viewingScope.getExcludedUnits()) {
            queryBuilder.mustNot(matchPhraseQuery(ORGANIZATION_IDS, excludedOrganizationId.toString()));
        }
        queryBuilder.mustNot(QueryBuilders.matchQuery(STATUS, APPROVED));
        return queryBuilder;
    }

    public SearchResponse doSearch(Query query, String index) throws BadGatewayException {
        try {
            SearchRequest searchRequest = query.toSearchRequest(index);
            return elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new BadGatewayException(NO_RESPONSE_FROM_INDEX);
        }
    }
}
