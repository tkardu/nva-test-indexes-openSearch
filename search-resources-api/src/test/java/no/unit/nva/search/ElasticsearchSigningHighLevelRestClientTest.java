package no.unit.nva.search;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.search.exception.SearchException;
import no.unit.nva.search.models.IndexDocument;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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


}
