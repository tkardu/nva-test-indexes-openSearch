package no.unit.nva.search;

import static nva.commons.core.ioutils.IoUtils.inputStreamFromResources;
import static nva.commons.core.ioutils.IoUtils.streamToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import no.unit.nva.search.exception.SearchException;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;

public class ElasticsearchSigningHighLevelRestClientTest {

    public static final String SAMPLE_TERM = "SampleSearchTerm";
    public static final int MAX_RESULTS = 100;
    private static final int SAMPLE_NUMBER_OF_RESULTS = 7;
    private static final String SAMPLE_JSON_RESPONSE = "{}";
    private static final int SAMPLE_FROM = 0;
    private static final String SAMPLE_ORDERBY = "orderByField";
    private static final String ELASTIC_SAMPLE_RESPONSE_FILE = "sample_elasticsearch_response.json";
    private static final int ELASTIC_ACTUAL_SAMPLE_NUMBER_OF_RESULTS = 2;

    @Test
    void constructorWithEnvironmentDefinedShouldCreateInstance() {
        ElasticSearchHighLevelRestClient elasticSearchRestClient = new ElasticSearchHighLevelRestClient();
        assertNotNull(elasticSearchRestClient);
    }

    @Test
    void searchSingleTermReturnsResponse() throws ApiGatewayException, IOException {
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.toString()).thenReturn(SAMPLE_JSON_RESPONSE);
        when(restHighLevelClient.search(any(), any())).thenReturn(searchResponse);
        ElasticSearchHighLevelRestClient elasticSearchRestClient =
            new ElasticSearchHighLevelRestClient(new RestHighLevelClientWrapper(restHighLevelClient));
        SearchResourcesResponse searchResourcesResponse =
            elasticSearchRestClient.searchSingleTerm(SAMPLE_TERM,
                                                     SAMPLE_NUMBER_OF_RESULTS,
                                                     SAMPLE_FROM,
                                                     SAMPLE_ORDERBY,
                                                     SortOrder.DESC);
        assertNotNull(searchResourcesResponse);
    }

    @Test
    void searchSingleTermReturnsResponseWithStatsFromElastic() throws ApiGatewayException, IOException {
        RestHighLevelClientWrapper restHighLevelClient = mock(RestHighLevelClientWrapper.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        String elasticSearchResponseJson = getElasticSSearchResponseAsString();
        when(searchResponse.toString()).thenReturn(elasticSearchResponseJson);
        when(restHighLevelClient.search(any(), any())).thenReturn(searchResponse);
        ElasticSearchHighLevelRestClient elasticSearchRestClient =
            new ElasticSearchHighLevelRestClient(restHighLevelClient);
        SearchResourcesResponse searchResourcesResponse =
            elasticSearchRestClient.searchSingleTerm(SAMPLE_TERM,
                                                     MAX_RESULTS,
                                                     SAMPLE_FROM,
                                                     SAMPLE_ORDERBY,
                                                     SortOrder.DESC);
        assertNotNull(searchResourcesResponse);
        assertEquals(searchResourcesResponse.getTotal(), ELASTIC_ACTUAL_SAMPLE_NUMBER_OF_RESULTS);
    }

    @Test
    void searchSingleTermReturnsErrorResponseWhenExceptionInDoSearch() throws IOException {
        RestHighLevelClientWrapper restHighLevelClient = mock(RestHighLevelClientWrapper.class);
        when(restHighLevelClient.search(any(), any())).thenThrow(new IOException());
        ElasticSearchHighLevelRestClient elasticSearchRestClient =
            new ElasticSearchHighLevelRestClient(restHighLevelClient);
        assertThrows(SearchException.class, () -> elasticSearchRestClient.searchSingleTerm(SAMPLE_TERM,
                                                                                           SAMPLE_NUMBER_OF_RESULTS,
                                                                                           SAMPLE_FROM,
                                                                                           SAMPLE_ORDERBY,
                                                                                           SortOrder.DESC));
    }

    private String getElasticSSearchResponseAsString() {
        return streamToString(inputStreamFromResources(ELASTIC_SAMPLE_RESPONSE_FILE));
    }
}
