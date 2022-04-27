package no.unit.nva.search;

import static no.unit.nva.search.models.SearchResourcesResponse.toSearchResourcesResponse;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import java.io.IOException;
import java.net.URI;
import no.unit.nva.search.models.SearchDocumentsQuery;
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
    public static final String PUBLICATION_STATUS = "publication.status";
    public static final String DRAFT = "DRAFT";
    public static final String DOCUMENT_TYPE = "type";
    public static final String DOI_REQUEST = "DoiRequest";
    public static final String PUBLICATION_CONVERSATION = "PublicationConversation";
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
    public SearchResourcesResponse searchSingleTerm(SearchDocumentsQuery query, String index)
        throws ApiGatewayException {
        var searchResponse = doSearch(query, index);
        return toSearchResourcesResponse(query.getRequestUri(), query.getSearchTerm(), searchResponse.toString());
    }

    public SearchResponse findResourcesForOrganizationIds(ViewingScope viewingScope,
                                                          int pageSize,
                                                          int pageNo,
                                                          String... index)
        throws BadGatewayException {
        try {
            SearchRequest searchRequest = createSearchRequestForResourcesWithOrganizationIds(viewingScope,
                                                                                             pageSize,
                                                                                             pageNo,
                                                                                             index);
            return elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new BadGatewayException(NO_RESPONSE_FROM_INDEX);
        }
    }

    public SearchResponse doSearch(SearchDocumentsQuery query, String index) throws BadGatewayException {
        try {
            SearchRequest searchRequest = query.toSearchRequest(index);
            return elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new BadGatewayException(NO_RESPONSE_FROM_INDEX);
        }
    }

    private SearchRequest createSearchRequestForResourcesWithOrganizationIds(
        ViewingScope viewingScope, int pageSize, int pageNo, String... indices) {
        BoolQueryBuilder queryBuilder = searchQueryBasedOnOrganizationIdsAndStatus(viewingScope);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(queryBuilder)
            .size(pageSize)
            .from(calculateFirstEntryIndex(pageSize, pageNo));

        return new SearchRequest(indices).source(searchSourceBuilder);
    }

    private int calculateFirstEntryIndex(int pageSize, int pageNo) {
        return pageSize * pageNo;
    }

    private BoolQueryBuilder searchQueryBasedOnOrganizationIdsAndStatus(ViewingScope viewingScope) {
        return new BoolQueryBuilder()
            .should(allSupportMessages(viewingScope))
            .should(nonApprovedDoiRequestsForPublishedPublications(viewingScope));
    }

    private BoolQueryBuilder allSupportMessages(ViewingScope viewingScope) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
            .must(QueryBuilders.matchQuery(DOCUMENT_TYPE, PUBLICATION_CONVERSATION))
            .must(existsQuery(ORGANIZATION_IDS));
        addViewingScope(viewingScope, queryBuilder);
        return queryBuilder;
    }

    private BoolQueryBuilder nonApprovedDoiRequestsForPublishedPublications(ViewingScope viewingScope) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
            .must(QueryBuilders.matchQuery(DOCUMENT_TYPE, DOI_REQUEST))
            .must(existsQuery(ORGANIZATION_IDS))
            .mustNot(QueryBuilders.matchQuery(STATUS, APPROVED))
            .mustNot(QueryBuilders.matchQuery(PUBLICATION_STATUS, DRAFT));
        addViewingScope(viewingScope, queryBuilder);
        return queryBuilder;
    }

    private void addViewingScope(ViewingScope viewingScope, BoolQueryBuilder queryBuilder) {
        for (URI includedOrganizationId : viewingScope.getIncludedUnits()) {
            queryBuilder.must(matchPhraseQuery(ORGANIZATION_IDS, includedOrganizationId.toString()));
        }
        for (URI excludedOrganizationId : viewingScope.getExcludedUnits()) {
            queryBuilder.mustNot(matchPhraseQuery(ORGANIZATION_IDS, excludedOrganizationId.toString()));
        }
    }
}
