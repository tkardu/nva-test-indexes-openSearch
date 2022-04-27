package no.unit.nva.search;

import static no.unit.nva.search.SearchClient.DOI_REQUEST;
import static no.unit.nva.search.SearchClientConfig.defaultSearchClient;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_ENDPOINT_INDEX;
import static no.unit.nva.testutils.RandomDataGenerator.randomInteger;
import static no.unit.nva.testutils.RandomDataGenerator.randomUri;
import static nva.commons.core.ioutils.IoUtils.inputStreamFromResources;
import static nva.commons.core.ioutils.IoUtils.streamToString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import no.unit.nva.search.models.SearchDocumentsQuery;
import no.unit.nva.search.models.SearchResourcesResponse;
import no.unit.nva.search.restclients.responses.ViewingScope;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.apigateway.exceptions.BadGatewayException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

class SearchClientTest {

    public static final String SAMPLE_TERM = "SampleSearchTerm";
    public static final int MAX_RESULTS = 100;
    public static final int DEFAULT_PAGE_SIZE = 100;
    public static final int DEFAULT_PAGE_NO = 0;
    private static final int SAMPLE_NUMBER_OF_RESULTS = 7;
    private static final String SAMPLE_JSON_RESPONSE = "{}";
    private static final int SAMPLE_FROM = 0;
    private static final String SAMPLE_ORDERBY = "orderByField";
    private static final String ELASTIC_SAMPLE_RESPONSE_FILE = "sample_elasticsearch_response.json";
    private static final int ELASTIC_ACTUAL_SAMPLE_NUMBER_OF_RESULTS = 2;
    private static final URI SAMPLE_REQUEST_URI = randomUri();

    @Test
    void constructorWithEnvironmentDefinedShouldCreateInstance() {
        SearchClient searchClient = defaultSearchClient();
        assertNotNull(searchClient);
    }

    @Test
    void shouldSendQueryWithAllNeededClauseForDoiRequestsTypeWhenSearchingForResources()
        throws ApiGatewayException {
        AtomicReference<SearchRequest> sentRequestBuffer = new AtomicReference<>();
        var restClientWrapper = new RestHighLevelClientWrapper((RestHighLevelClient) null) {
            @Override
            public SearchResponse search(SearchRequest searchRequest, RequestOptions requestOptions) {
                sentRequestBuffer.set(searchRequest);
                var searchResponse = mock(SearchResponse.class);
                when(searchResponse.toString()).thenReturn(SAMPLE_JSON_RESPONSE);
                return searchResponse;
            }
        };

        var searchClient = new SearchClient(restClientWrapper);
        searchClient.findResourcesForOrganizationIds(generateSampleViewingScope(),
                                                     DEFAULT_PAGE_SIZE,
                                                     DEFAULT_PAGE_NO,
                                                     ELASTICSEARCH_ENDPOINT_INDEX);
        var sentRequest = sentRequestBuffer.get();
        var rulesForExcludingDoiRequests = listAllRulesForExcludingDoiRequests(sentRequest);
        var mustExcludeApprovedDoiRequests =
            rulesForExcludingDoiRequests.stream().anyMatch(condition -> condition.value().equals(
                "APPROVED"));
        var mustExcludeDoiRequestsForDraftPublications =
            rulesForExcludingDoiRequests.stream().anyMatch(condition -> condition.value().equals("DRAFT"));

        assertTrue(mustExcludeApprovedDoiRequests, "Could not find rule for excluding APPROVED DoiRequests");
        assertTrue(mustExcludeDoiRequestsForDraftPublications,
                   "Could not find rule for excluding  DoiRequests for Draft Publications");
    }

    @Test
    void shouldSendQueryWithAllNeededClauseForPublicationConversationTypeWhenSearchingForResources()
        throws ApiGatewayException {
        AtomicReference<SearchRequest> sentRequestBuffer = new AtomicReference<>();
        var restClientWrapper = new RestHighLevelClientWrapper((RestHighLevelClient) null) {
            @Override
            public SearchResponse search(SearchRequest searchRequest, RequestOptions requestOptions) {
                sentRequestBuffer.set(searchRequest);
                var searchResponse = mock(SearchResponse.class);
                when(searchResponse.toString()).thenReturn(SAMPLE_JSON_RESPONSE);
                return searchResponse;
            }
        };

        SearchClient searchClient = new SearchClient(restClientWrapper);
        searchClient.findResourcesForOrganizationIds(generateSampleViewingScope(),
                                                     DEFAULT_PAGE_SIZE,
                                                     DEFAULT_PAGE_NO,
                                                     ELASTICSEARCH_ENDPOINT_INDEX);
        var sentRequest = sentRequestBuffer.get();
        var query = sentRequest.source().query();
        var publicationConversationTypeClauseIndexInQuery = 0;
        var doiRequestsQueryBuilder = ((BoolQueryBuilder) ((BoolQueryBuilder) query).should()
            .get(publicationConversationTypeClauseIndexInQuery));
        var documentTypeIndexInQueryBuilderMustClause = 0;
        var expectedDocumentTypeInMustClause = ((MatchQueryBuilder) doiRequestsQueryBuilder.must()
            .get(documentTypeIndexInQueryBuilderMustClause)).value();
        assertThat(expectedDocumentTypeInMustClause, is(equalTo("PublicationConversation")));
    }

    @Test
    void searchSingleTermReturnsResponse() throws ApiGatewayException, IOException {
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.toString()).thenReturn(SAMPLE_JSON_RESPONSE);
        when(restHighLevelClient.search(any(), any())).thenReturn(searchResponse);
        SearchClient searchClient =
            new SearchClient(new RestHighLevelClientWrapper(restHighLevelClient));
        SearchResourcesResponse searchResourcesResponse =
            searchClient.searchSingleTerm(generateSampleQuery(), ELASTICSEARCH_ENDPOINT_INDEX);
        assertNotNull(searchResourcesResponse);
    }

    @Test
    void shouldReturnSearchResponseWhenSearchingWithOrganizationIds() throws ApiGatewayException, IOException {
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.toString()).thenReturn(SAMPLE_JSON_RESPONSE);
        when(restHighLevelClient.search(any(), any())).thenReturn(searchResponse);
        SearchClient searchClient =
            new SearchClient(new RestHighLevelClientWrapper(restHighLevelClient));
        SearchResponse response =
            searchClient.findResourcesForOrganizationIds(generateSampleViewingScope(),
                                                         DEFAULT_PAGE_SIZE,
                                                         DEFAULT_PAGE_NO,
                                                         ELASTICSEARCH_ENDPOINT_INDEX);
        assertNotNull(response);
    }

    @Test
    void shouldSendRequestWithSuppliedPageSizeWhenSearchingForResources() throws ApiGatewayException {
        AtomicReference<SearchRequest> sentRequestBuffer = new AtomicReference<>();
        var restClientWrapper = new RestHighLevelClientWrapper((RestHighLevelClient) null) {
            @Override
            public SearchResponse search(SearchRequest searchRequest, RequestOptions requestOptions) {
                sentRequestBuffer.set(searchRequest);
                var searchResponse = mock(SearchResponse.class);
                when(searchResponse.toString()).thenReturn(SAMPLE_JSON_RESPONSE);
                return searchResponse;
            }
        };

        SearchClient searchClient = new SearchClient(restClientWrapper);
        int resultSize = 1 + randomInteger(1000);
        searchClient.findResourcesForOrganizationIds(generateSampleViewingScope(),
                                                     resultSize,
                                                     DEFAULT_PAGE_NO,
                                                     ELASTICSEARCH_ENDPOINT_INDEX);
        var sentRequest = sentRequestBuffer.get();
        var actualRequestedSize = sentRequest.source().size();
        assertThat(actualRequestedSize, is(equalTo(resultSize)));
    }

    @Test
    void shouldSendRequestWithFirstEntryIndexCalculatedBySuppliedPageSizeAndPageNumber() throws ApiGatewayException {
        AtomicReference<SearchRequest> sentRequestBuffer = new AtomicReference<>();
        var restClientWrapper = new RestHighLevelClientWrapper((RestHighLevelClient) null) {
            @Override
            public SearchResponse search(SearchRequest searchRequest, RequestOptions requestOptions) {
                sentRequestBuffer.set(searchRequest);
                var searchResponse = mock(SearchResponse.class);
                when(searchResponse.toString()).thenReturn(SAMPLE_JSON_RESPONSE);
                return searchResponse;
            }
        };

        SearchClient searchClient = new SearchClient(restClientWrapper);
        int pageNo = randomInteger(100);
        searchClient.findResourcesForOrganizationIds(generateSampleViewingScope(),
                                                     DEFAULT_PAGE_SIZE,
                                                     pageNo,
                                                     ELASTICSEARCH_ENDPOINT_INDEX);
        var sentRequest = sentRequestBuffer.get();
        var actualResultsFrom = sentRequest.source().from();
        var resultsFrom = pageNo * DEFAULT_PAGE_SIZE;
        assertThat(actualResultsFrom, is(equalTo(resultsFrom)));
    }

    @Test
    void searchSingleTermReturnsResponseWithStatsFromElastic() throws ApiGatewayException, IOException {
        RestHighLevelClientWrapper restHighLevelClient = mock(RestHighLevelClientWrapper.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        String elasticSearchResponseJson = generateElasticSearchResponseAsString();
        when(searchResponse.toString()).thenReturn(elasticSearchResponseJson);
        when(restHighLevelClient.search(any(), any())).thenReturn(searchResponse);
        SearchClient searchClient = new SearchClient(restHighLevelClient);

        SearchDocumentsQuery queryWithMaxResults = new SearchDocumentsQuery(SAMPLE_TERM,
                                                                            MAX_RESULTS,
                                                                            SAMPLE_FROM,
                                                                            SAMPLE_ORDERBY,
                                                                            SortOrder.DESC,
                                                                            SAMPLE_REQUEST_URI);

        SearchResourcesResponse searchResourcesResponse =
            searchClient.searchSingleTerm(queryWithMaxResults, ELASTICSEARCH_ENDPOINT_INDEX);
        assertNotNull(searchResourcesResponse);
        assertEquals(searchResourcesResponse.getSize(), ELASTIC_ACTUAL_SAMPLE_NUMBER_OF_RESULTS);
    }

    @Test
    void searchSingleTermReturnsErrorResponseWhenExceptionInDoSearch() throws IOException {
        RestHighLevelClientWrapper restHighLevelClient = mock(RestHighLevelClientWrapper.class);
        when(restHighLevelClient.search(any(), any())).thenThrow(new IOException());
        SearchClient searchClient = new SearchClient(restHighLevelClient);
        assertThrows(BadGatewayException.class,
                     () -> searchClient.searchSingleTerm(generateSampleQuery(), ELASTICSEARCH_ENDPOINT_INDEX));
    }

    @NotNull
    private List<MatchQueryBuilder> listAllRulesForExcludingDoiRequests(SearchRequest sentRequest) {
        return listAllDisjunctiveRulesForMatchingDocuments(sentRequest)
            .filter(this::keepOnlyTheDoiRequestRelatedConditions)
            .flatMap(this::listTheExclusionRulesForDoiRequests)
            .filter(this::keepOnlyMatchTypeRules)
            .map(matches -> (MatchQueryBuilder) matches)
            .collect(Collectors.toList());
    }

    private Stream<QueryBuilder> listTheExclusionRulesForDoiRequests(BoolQueryBuilder q) {
        return q.mustNot().stream();
    }

    private Stream<BoolQueryBuilder> listAllDisjunctiveRulesForMatchingDocuments(SearchRequest sentRequest) {
        return booleanQuery(sentRequest.source().query()).should()
            .stream()
            .map(queyClause -> (BoolQueryBuilder) queyClause);
    }

    private boolean keepOnlyMatchTypeRules(QueryBuilder condition) {
        return condition instanceof MatchQueryBuilder;
    }

    private boolean keepOnlyTheDoiRequestRelatedConditions(BoolQueryBuilder q) {
        return
            q.must()
                .stream()
                .filter(this::keepOnlyMatchTypeRules)
                .map(match -> (MatchQueryBuilder) match)
                .anyMatch(match -> match.value().equals(DOI_REQUEST));
    }

    private BoolQueryBuilder booleanQuery(QueryBuilder queryBuilder) {
        return (BoolQueryBuilder) queryBuilder;
    }

    private ViewingScope generateSampleViewingScope() {
        ViewingScope viewingScope = new ViewingScope();
        viewingScope.setIncludedUnits(Set.of(randomUri(), randomUri()));
        viewingScope.setExcludedUnits(Set.of(randomUri()));
        return viewingScope;
    }

    private SearchDocumentsQuery generateSampleQuery() {
        return new SearchDocumentsQuery(SAMPLE_TERM,
                                        SAMPLE_NUMBER_OF_RESULTS,
                                        SAMPLE_FROM,
                                        SAMPLE_ORDERBY,
                                        SortOrder.DESC,
                                        SAMPLE_REQUEST_URI);
    }

    private String generateElasticSearchResponseAsString() {
        return streamToString(inputStreamFromResources(ELASTIC_SAMPLE_RESPONSE_FILE));
    }
}
