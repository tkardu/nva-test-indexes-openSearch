package no.unit.nva.search;

import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_INDEX_KEY;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_REGION_KEY;
import static nva.commons.core.ioutils.IoUtils.inputStreamFromResources;
import static nva.commons.core.ioutils.IoUtils.streamToString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.List;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.search.exception.SearchException;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.core.Environment;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ElasticsearchSigningHighLevelRestClientTest {

    private static final String elasticSearchEndpoint = "http://localhost";
    public static final String SAMPLE_TERM = "SampleSearchTerm";
    private static final int SAMPLE_NUMBER_OF_RESULTS = 7;
    private static final String SAMPLE_JSON_RESPONSE = "{}";
    private static final int SAMPLE_FROM = 0;
    private static final String SAMPLE_ORDERBY = "orderByField";
    private static final String ELASTIC_SAMPLE_RESPONSE_FILE = "sample_elasticsearch_response.json";
    private static final int ELASTIC_ACTUAL_SAMPLE_NUMBER_OF_RESULTS = 2;
    public static final int MAX_RESULTS = 100;

    ElasticSearchHighLevelRestClient elasticSearchRestClient;
    private Environment environment;

    private void initEnvironment() {
        when(environment.readEnv(ELASTICSEARCH_ENDPOINT_ADDRESS_KEY)).thenReturn(elasticSearchEndpoint);
        when(environment.readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY)).thenReturn("resources");
        when(environment.readEnv(ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY)).thenReturn("https");
    }

    /**
     * Set up test environment.
     **/
    @BeforeEach
    void init() {
        environment = mock(Environment.class);
        initEnvironment();
        elasticSearchRestClient = new ElasticSearchHighLevelRestClient(environment);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void defaultConstructorWithEnvironmentIsNullShouldFail() {
        assertThrows(NullPointerException.class, () -> new ElasticSearchHighLevelRestClient(null));
    }

    @Test
    void constructorWithEnvironmentDefinedShouldCreateInstance() {
        ElasticSearchHighLevelRestClient elasticSearchRestClient = new ElasticSearchHighLevelRestClient(environment);
        assertNotNull(elasticSearchRestClient);
    }


    @Test
    void searchSingleTermReturnsResponse() throws ApiGatewayException, IOException {

        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.toString()).thenReturn(SAMPLE_JSON_RESPONSE);
        when(restHighLevelClient.search(any(), any())).thenReturn(searchResponse);
        ElasticSearchHighLevelRestClient elasticSearchRestClient =
                new ElasticSearchHighLevelRestClient(environment, restHighLevelClient);
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

        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        String elasticSearchResponseJson = getElasticSEarchResponseAsString();
        when(searchResponse.toString()).thenReturn(elasticSearchResponseJson);
        when(restHighLevelClient.search(any(), any())).thenReturn(searchResponse);
        ElasticSearchHighLevelRestClient elasticSearchRestClient =
                new ElasticSearchHighLevelRestClient(environment, restHighLevelClient);
        SearchResourcesResponse searchResourcesResponse =
                elasticSearchRestClient.searchSingleTerm(SAMPLE_TERM,
                        MAX_RESULTS,
                        SAMPLE_FROM,
                        SAMPLE_ORDERBY,
                        SortOrder.DESC);
        assertNotNull(searchResourcesResponse);
        assertEquals(searchResourcesResponse.getTotal(), ELASTIC_ACTUAL_SAMPLE_NUMBER_OF_RESULTS);
    }

    private String getElasticSEarchResponseAsString() {
        return streamToString(inputStreamFromResources(ELASTIC_SAMPLE_RESPONSE_FILE));
    }


    @Test
    void searchSingleTermReturnsErrorResponseWhenExceptionInDoSearch() throws ApiGatewayException, IOException {

        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        when(restHighLevelClient.search(any(), any())).thenThrow(new IOException());

        ElasticSearchHighLevelRestClient elasticSearchRestClient =
                new ElasticSearchHighLevelRestClient(environment, restHighLevelClient);

        assertThrows(SearchException.class, () -> elasticSearchRestClient.searchSingleTerm(SAMPLE_TERM,
                        SAMPLE_NUMBER_OF_RESULTS,
                        SAMPLE_FROM,
                        SAMPLE_ORDERBY,
                        SortOrder.DESC));

    }



    @Test
    void addDocumentToIndexThrowsException() throws IOException {

        IndexDocument indexDocument = mock(IndexDocument.class);
        doThrow(RuntimeException.class).when(indexDocument).toJsonString();
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        when(restHighLevelClient.update(any(), any())).thenThrow(new RuntimeException());
        ElasticSearchHighLevelRestClient elasticSearchRestClient =
                new ElasticSearchHighLevelRestClient(environment, restHighLevelClient);

        assertThrows(SearchException.class, () -> elasticSearchRestClient.addDocumentToIndex(indexDocument));
    }

    @Test
    void removeDocumentThrowsException() throws IOException {

        IndexDocument indexDocument = mock(IndexDocument.class);
        doThrow(RuntimeException.class).when(indexDocument).toJsonString();
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        when(restHighLevelClient.update(any(), any())).thenThrow(new RuntimeException());
        ElasticSearchHighLevelRestClient elasticSearchRestClient =
                new ElasticSearchHighLevelRestClient(environment, restHighLevelClient);

        assertThrows(SearchException.class, () -> elasticSearchRestClient.removeDocumentFromIndex(""));
    }

    @Test
    void removeDocumentReturnsDocumentNotFoundWhenNoDocumentMatchesIdentifier() throws IOException,
            SearchException {

        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        DeleteResponse nothingFoundResponse = mock(DeleteResponse.class);
        when(nothingFoundResponse.getResult()).thenReturn(DocWriteResponse.Result.NOT_FOUND);
        when(restHighLevelClient.delete(any(), any())).thenReturn(nothingFoundResponse);
        ElasticSearchHighLevelRestClient elasticSearchRestClient =
                new ElasticSearchHighLevelRestClient(environment, restHighLevelClient);
        elasticSearchRestClient.removeDocumentFromIndex("1234");
    }

    @Test
    void addDocumentToIndex() throws IOException, SearchException {

        UpdateResponse updateResponse = mock(UpdateResponse.class);
        IndexDocument mockDocument = mock(IndexDocument.class);
        when(mockDocument.toJsonString()).thenReturn("{}");
        when(mockDocument.getId()).thenReturn(SortableIdentifier.next());
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        when(restHighLevelClient.update(any(), any())).thenReturn(updateResponse);

        ElasticSearchHighLevelRestClient elasticSearchRestClient =
                new ElasticSearchHighLevelRestClient(environment, restHighLevelClient);

        elasticSearchRestClient.addDocumentToIndex(mockDocument);
    }



}
