package no.unit.nva.search;

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import no.unit.nva.search.models.SearchDocumentsQuery;
import no.unit.nva.search.models.SearchResourcesResponse;
import no.unit.nva.search.restclients.responses.ViewingScope;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.apigateway.exceptions.BadGatewayException;
import nva.commons.core.paths.UriWrapper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;

class SearchClientTest {

    public static final String SAMPLE_TERM = "SampleSearchTerm";
    public static final int MAX_RESULTS = 100;
    private static final int SAMPLE_NUMBER_OF_RESULTS = 7;
    private static final String SAMPLE_JSON_RESPONSE = "{}";
    private static final int SAMPLE_FROM = 0;
    private static final String SAMPLE_ORDERBY = "orderByField";
    private static final String ELASTIC_SAMPLE_RESPONSE_FILE = "sample_elasticsearch_response.json";
    private static final int ELASTIC_ACTUAL_SAMPLE_NUMBER_OF_RESULTS = 2;
    public static final int DEFAULT_RESULT_SIZE = 100;
    private static final URI SAMPLE_REQUEST_URI = getSampleRequestUri();

    @Test
    void constructorWithEnvironmentDefinedShouldCreateInstance() {
        SearchClient searchClient = defaultSearchClient();
        assertNotNull(searchClient);
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
            searchClient.searchSingleTerm(getSampleQuery(), ELASTICSEARCH_ENDPOINT_INDEX);
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
            searchClient.findResourcesForOrganizationIds(getSampleViewingScope(),
                                                         DEFAULT_RESULT_SIZE, 0,
                                                         ELASTICSEARCH_ENDPOINT_INDEX);
        assertNotNull(response);
    }

    @Test
    void shouldReturnResponseWithMaxSizeEqualToSpecifiedSizeWhenSearchingForResources() throws ApiGatewayException {
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
        searchClient.findResourcesForOrganizationIds(getSampleViewingScope(),
                                                     resultSize, 0,
                                                     ELASTICSEARCH_ENDPOINT_INDEX);
        var sentRequest = sentRequestBuffer.get();
        var actualRequestedSize = sentRequest.source().size();
        assertThat(actualRequestedSize, is(equalTo(resultSize)));
    }

    @Test
    void shouldSentRequestWithDefinedFromWhenSearchingForResourcesWithPageNo() throws ApiGatewayException {
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
        searchClient.findResourcesForOrganizationIds(getSampleViewingScope(),
                                                     DEFAULT_RESULT_SIZE, pageNo,
                                                     ELASTICSEARCH_ENDPOINT_INDEX);
        var sentRequest = sentRequestBuffer.get();
        var actualResultsFrom = sentRequest.source().from();
        var resultsFrom = pageNo * DEFAULT_RESULT_SIZE;
        assertThat(actualResultsFrom, is(equalTo(resultsFrom)));
    }

    @Test
    void searchSingleTermReturnsResponseWithStatsFromElastic() throws ApiGatewayException, IOException {
        RestHighLevelClientWrapper restHighLevelClient = mock(RestHighLevelClientWrapper.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        String elasticSearchResponseJson = getElasticSSearchResponseAsString();
        when(searchResponse.toString()).thenReturn(elasticSearchResponseJson);
        when(restHighLevelClient.search(any(), any())).thenReturn(searchResponse);
        SearchClient searchClient =
            new SearchClient(restHighLevelClient);

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
        SearchClient searchClient =
            new SearchClient(restHighLevelClient);
        assertThrows(BadGatewayException.class,
                     () -> searchClient.searchSingleTerm(getSampleQuery(), ELASTICSEARCH_ENDPOINT_INDEX));
    }

    private static URI getSampleRequestUri() {
        return new UriWrapper("https", "localhost")
            .addChild("search")
            .addChild("resources")
            .getUri();
    }

    private ViewingScope getSampleViewingScope() {
        ViewingScope viewingScope = new ViewingScope();
        viewingScope.setIncludedUnits(Set.of(randomUri(), randomUri()));
        viewingScope.setExcludedUnits(Set.of(randomUri()));
        return viewingScope;
    }

    private SearchDocumentsQuery getSampleQuery() {
        return new SearchDocumentsQuery(SAMPLE_TERM,
                                        SAMPLE_NUMBER_OF_RESULTS,
                                        SAMPLE_FROM,
                                        SAMPLE_ORDERBY,
                                        SortOrder.DESC,
                                        SAMPLE_REQUEST_URI);
    }

    private String getElasticSSearchResponseAsString() {
        return streamToString(inputStreamFromResources(ELASTIC_SAMPLE_RESPONSE_FILE));
    }
}
