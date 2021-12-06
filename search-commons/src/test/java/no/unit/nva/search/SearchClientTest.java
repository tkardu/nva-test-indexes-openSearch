package no.unit.nva.search;

import no.unit.nva.search.models.Query;
import no.unit.nva.search.models.SearchResourcesResponse;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.apigateway.exceptions.BadGatewayException;
import nva.commons.core.paths.UriWrapper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static no.unit.nva.search.SearchClientConfig.defaultSearchClient;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_ENDPOINT_INDEX;
import static nva.commons.core.ioutils.IoUtils.inputStreamFromResources;
import static nva.commons.core.ioutils.IoUtils.streamToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchClientTest {

    public static final String SAMPLE_TERM = "SampleSearchTerm";
    public static final int MAX_RESULTS = 100;
    private static final int SAMPLE_NUMBER_OF_RESULTS = 7;
    private static final String SAMPLE_JSON_RESPONSE = "{}";
    private static final int SAMPLE_FROM = 0;
    private static final String SAMPLE_ORDERBY = "orderByField";
    private static final String ELASTIC_SAMPLE_RESPONSE_FILE = "sample_elasticsearch_response.json";
    private static final int ELASTIC_ACTUAL_SAMPLE_NUMBER_OF_RESULTS = 2;
    private static final UriWrapper SAMPLE_REQUEST_URI = getSampleRequestUri();

    private static UriWrapper getSampleRequestUri() {
        return new UriWrapper("https", "localhost")
                .addChild("search")
                .addChild("resources");
    }

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

    private Query getSampleQuery() {
        return new Query(SAMPLE_TERM,
                SAMPLE_NUMBER_OF_RESULTS,
                SAMPLE_FROM,
                SAMPLE_ORDERBY,
                SortOrder.DESC,
                SAMPLE_REQUEST_URI);
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

        Query queryWithMaxResults = new Query(SAMPLE_TERM,
                MAX_RESULTS,
                SAMPLE_FROM,
                SAMPLE_ORDERBY,
                SortOrder.DESC,
                SAMPLE_REQUEST_URI);

        SearchResourcesResponse searchResourcesResponse =
            searchClient.searchSingleTerm(queryWithMaxResults, ELASTICSEARCH_ENDPOINT_INDEX);
        assertNotNull(searchResourcesResponse);
        assertEquals(searchResourcesResponse.getTotal(), ELASTIC_ACTUAL_SAMPLE_NUMBER_OF_RESULTS);
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

    private String getElasticSSearchResponseAsString() {
        return streamToString(inputStreamFromResources(ELASTIC_SAMPLE_RESPONSE_FILE));
    }
}
