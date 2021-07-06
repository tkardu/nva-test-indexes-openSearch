package no.unit.nva.search;

import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_ENDPOINT_INDEX;
import static nva.commons.core.ioutils.IoUtils.inputStreamFromResources;
import static nva.commons.core.ioutils.IoUtils.streamToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.IOException;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.search.exception.SearchException;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
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
    public static final String[] EMPTY_INDICES_LIST = {};

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
    void searchSingleTermReturnsErrorResponseWhenExceptionInDoSearch() throws ApiGatewayException, IOException {

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

    @Test
    void addDocumentToIndexThrowsException() throws IOException {

        IndexDocument indexDocument = mock(IndexDocument.class);
        doThrow(RuntimeException.class).when(indexDocument).toJsonString();
        RestHighLevelClientWrapper restHighLevelClient = mock(RestHighLevelClientWrapper.class);
        when(restHighLevelClient.update(any(), any())).thenThrow(new RuntimeException());
        ElasticSearchHighLevelRestClient elasticSearchRestClient =
            new ElasticSearchHighLevelRestClient(restHighLevelClient);

        assertThrows(SearchException.class, () -> elasticSearchRestClient.addDocumentToIndex(indexDocument));
    }

    @Test
    void removeDocumentThrowsException() throws IOException {

        IndexDocument indexDocument = mock(IndexDocument.class);
        doThrow(RuntimeException.class).when(indexDocument).toJsonString();
        RestHighLevelClientWrapper restHighLevelClient = mock(RestHighLevelClientWrapper.class);
        when(restHighLevelClient.update(any(), any())).thenThrow(new RuntimeException());
        ElasticSearchHighLevelRestClient elasticSearchRestClient =
            new ElasticSearchHighLevelRestClient(restHighLevelClient);

        assertThrows(SearchException.class, () -> elasticSearchRestClient.removeDocumentFromIndex(""));
    }

    @Test
    void removeDocumentReturnsDocumentNotFoundWhenNoDocumentMatchesIdentifier() throws IOException,
                                                                                       SearchException {

        RestHighLevelClientWrapper restHighLevelClient = mock(RestHighLevelClientWrapper.class);
        DeleteResponse nothingFoundResponse = mock(DeleteResponse.class);
        when(nothingFoundResponse.getResult()).thenReturn(DocWriteResponse.Result.NOT_FOUND);
        when(restHighLevelClient.delete(any(), any())).thenReturn(nothingFoundResponse);
        ElasticSearchHighLevelRestClient elasticSearchRestClient =
            new ElasticSearchHighLevelRestClient(restHighLevelClient);
        elasticSearchRestClient.removeDocumentFromIndex("1234");
    }

    @Test
    void addDocumentToIndex() throws IOException, SearchException {

        UpdateResponse updateResponse = mock(UpdateResponse.class);
        IndexDocument mockDocument = mock(IndexDocument.class);
        when(mockDocument.toJsonString()).thenReturn("{}");
        when(mockDocument.getId()).thenReturn(SortableIdentifier.next());
        RestHighLevelClientWrapper restHighLevelClient = mock(RestHighLevelClientWrapper.class);
        when(restHighLevelClient.update(any(), any())).thenReturn(updateResponse);

        ElasticSearchHighLevelRestClient elasticSearchRestClient =
            new ElasticSearchHighLevelRestClient(restHighLevelClient);

        elasticSearchRestClient.addDocumentToIndex(mockDocument);
    }

    @Test
    public void prepareIndexForBatchInsertUpdatesIndexWhenResourcesIndexExists() throws IOException {

        RestHighLevelClientWrapper esClient = mock(RestHighLevelClientWrapper.class);
        GetIndexResponse mockResponse = mock(GetIndexResponse.class);
        when(mockResponse.getIndices()).thenReturn(new String[]{ELASTICSEARCH_ENDPOINT_INDEX});

        IndicesClientWrapper mockIndicesClient = mock(IndicesClientWrapper.class);
        when(mockIndicesClient.get(any(GetIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(mockResponse);

        when(esClient.indices()).thenReturn(mockIndicesClient);
        ElasticSearchHighLevelRestClient client = new ElasticSearchHighLevelRestClient(esClient);
        client.prepareIndexForBatchInsert();
        verify(mockIndicesClient, times(1))
            .putSettings(any(UpdateSettingsRequest.class), any(RequestOptions.class));
    }

    @Test
    public void prepareIndexForBatchInsertCreatesIndexWhenResourcesIndexDoesNotExist() throws IOException {
        RestHighLevelClientWrapper esClient = mock(RestHighLevelClientWrapper.class);
        IndicesClientWrapper mockIndicesClient = mock(IndicesClientWrapper.class);
        GetIndexResponse mockResponse = mock(GetIndexResponse.class);
        when(mockResponse.getIndices()).thenReturn(EMPTY_INDICES_LIST);
        when(mockIndicesClient.get(any(GetIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(mockResponse);
        when(esClient.indices()).thenReturn(mockIndicesClient);

        ElasticSearchHighLevelRestClient client = new ElasticSearchHighLevelRestClient(esClient);
        client.prepareIndexForBatchInsert();
        verify(mockIndicesClient, times(1))
            .create(any(CreateIndexRequest.class), any(RequestOptions.class));
    }

    private String getElasticSSearchResponseAsString() {
        return streamToString(inputStreamFromResources(ELASTIC_SAMPLE_RESPONSE_FILE));
    }
}
