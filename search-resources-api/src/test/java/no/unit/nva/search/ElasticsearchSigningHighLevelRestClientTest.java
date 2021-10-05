package no.unit.nva.search;

import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.search.exception.SearchException;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticsearchSigningHighLevelRestClientTest {

    public static final String SAMPLE_TERM = "SampleSearchTerm";
    private static final int SAMPLE_NUMBER_OF_RESULTS = 7;
    private static final String SAMPLE_JSON_RESPONSE = "{}";
    private static final int SAMPLE_FROM = 0;
    private static final String SAMPLE_ORDERBY = "orderByField";

    ElasticSearchHighLevelRestClient elasticSearchRestClient;

    /**
     * Set up test environment.
     **/
    @BeforeEach
    void init() {
        elasticSearchRestClient = new ElasticSearchHighLevelRestClient();
    }

    @Test
    void constructorWithEnvironmentDefinedShouldCreateInstance() {
        ElasticSearchHighLevelRestClient elasticSearchRestClient = new ElasticSearchHighLevelRestClient();
        assertNotNull(elasticSearchRestClient);
    }

    @Test
    void searchSingleTermReturnsResponse() throws ApiGatewayException, IOException {

        RestHighLevelClientWrapper restHighLevelClient = mock(RestHighLevelClientWrapper.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.toString()).thenReturn(SAMPLE_JSON_RESPONSE);
        when(restHighLevelClient.search(any(), any())).thenReturn(searchResponse);
        ElasticSearchHighLevelRestClient elasticSearchRestClient =
            new ElasticSearchHighLevelRestClient(restHighLevelClient);
        SearchResourcesResponse searchResourcesResponse =
            elasticSearchRestClient.searchSingleTerm(SAMPLE_TERM,
                                                     SAMPLE_NUMBER_OF_RESULTS,
                                                     SAMPLE_FROM,
                                                     SAMPLE_ORDERBY,
                                                     SortOrder.DESC);
        assertNotNull(searchResourcesResponse);
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
        when(mockDocument.getIdentifier()).thenReturn(SortableIdentifier.next());
        RestHighLevelClientWrapper restHighLevelClient = mock(RestHighLevelClientWrapper.class);
        when(restHighLevelClient.update(any(), any())).thenReturn(updateResponse);

        ElasticSearchHighLevelRestClient elasticSearchRestClient =
            new ElasticSearchHighLevelRestClient(restHighLevelClient);

        elasticSearchRestClient.addDocumentToIndex(mockDocument);
    }
}
