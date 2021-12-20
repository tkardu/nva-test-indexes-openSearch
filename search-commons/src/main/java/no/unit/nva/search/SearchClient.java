package no.unit.nva.search;

import no.unit.nva.search.models.Query;
import no.unit.nva.search.models.SearchResourcesResponse;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.apigateway.exceptions.BadGatewayException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

import static no.unit.nva.search.models.SearchResourcesResponse.toSearchResourcesResponse;

public class SearchClient {

    public static final String NO_RESPONSE_FROM_INDEX = "No response from index";
    public static final String ORGANIZATION_IDS = "organizationIds";
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

    public SearchResponse doSearch(Query query, String index, Set<URI> organizationIds) throws BadGatewayException {
        try {
            BoolQueryBuilder queryBuilder = getBoolQueryBuilder(organizationIds);
            SearchRequest searchRequest = getSearchRequest(index, queryBuilder);
            return elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new BadGatewayException(NO_RESPONSE_FROM_INDEX);
        }
    }

    private SearchRequest getSearchRequest(String index, QueryBuilder queryBuilder) {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    private BoolQueryBuilder getBoolQueryBuilder(Set<URI> organizationIds) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        queryBuilder.must(QueryBuilders.existsQuery(ORGANIZATION_IDS));
        queryBuilder.minimumShouldMatch(1);
        for (URI organizationId : organizationIds) {
            queryBuilder.should(QueryBuilders
                    .matchQuery(ORGANIZATION_IDS, organizationId.toString())
                    .fuzziness(Fuzziness.ZERO)
                    .fuzzyTranspositions(false)
            );
        }
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
