package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.search.constants.ApplicationConstants;
import nva.commons.apigateway.GatewayResponse;
import nva.commons.apigateway.RequestInfo;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.core.Environment;
import nva.commons.core.JsonUtils;
import nva.commons.core.ioutils.IoUtils;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static nva.commons.core.ioutils.IoUtils.stringFromResources;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchResourcesApiHandlerTest {

    public static final String SAMPLE_SEARCH_TERM = "searchTerm";
    public static final String SAMPLE_ELASTICSEARCH_RESPONSE_JSON = "sample_elasticsearch_response.json";
    public static final String EMPTY_ELASTICSEARCH_RESPONSE_JSON = "empty_elasticsearch_response.json";
    public static final String ROUNDTRIP_RESPONSE_JSON = "roundtripResponse.json";
    public static final URI EXAMPLE_CONTEXT = URI.create("https://example.org/search");
    public static final URI EXAMPLE_ID = URI.create("https://example.org/search?query=aTerm");
    public static final List<JsonNode> SAMPLE_HITS = Collections.emptyList();
    public static final int SAMPLE_TOOK = 0;
    public static final int SAMPLE_TOTAL = 0;

    private final Environment environment = new Environment();
    private SearchResourcesApiHandler searchResourcesApiHandler;

    @BeforeEach
    void init() {
        searchResourcesApiHandler = new SearchResourcesApiHandler();
    }

    @Test
    void getSuccessStatusCodeReturnsOK() {
        SearchResourcesResponse response = new SearchResourcesResponse(EXAMPLE_CONTEXT,
                                                                       EXAMPLE_ID,
                                                                       SAMPLE_TOOK,
                                                                       SAMPLE_TOTAL,
                                                                       SAMPLE_HITS);
        Integer statusCode = searchResourcesApiHandler.getSuccessStatusCode(null, response);
        assertEquals(statusCode, HttpStatus.SC_OK);
    }

    @Test
    void handlerReturnsSearchResultsWhenQueryIsSingleTerm() throws ApiGatewayException, IOException {
        var elasticSearchClient = new ElasticSearchHighLevelRestClient(setUpRestHighLevelClient());
        var handler = new SearchResourcesApiHandler(environment, elasticSearchClient);
        var actual = handler.processInput(null, getRequestInfo(), mock(Context.class));
        var expected = objectMapperWithEmpty.readValue(stringFromResources(Path.of(ROUNDTRIP_RESPONSE_JSON)),
                                        SearchResourcesResponse.class);
        assertEquals(expected, actual);
    }

    @Test
    void handlerReturnsSearchResultsWithEmptyHitsWhenQueryResultIsEmpty() throws IOException {
        var elasticSearchClient =
            new ElasticSearchHighLevelRestClient(setUpRestHighLevelClientWithEmptyResponse());
        var handler = new SearchResourcesApiHandler(environment, elasticSearchClient);
        var inputStream = IoUtils.inputStreamFromResources(EMPTY_ELASTICSEARCH_RESPONSE_JSON);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        handler.handleRequest(inputStream, outputStream, mock(Context.class));
        GatewayResponse<SearchResourcesResponse> gatewayResponse = GatewayResponse.fromOutputStream(outputStream);
        SearchResourcesResponse body = gatewayResponse.getBodyObject(SearchResourcesResponse.class);

        assertNotNull(gatewayResponse.getHeaders());
        assertEquals(gatewayResponse.getStatusCode(), HttpStatus.SC_OK);
        assertThat(body.getTotal(), is(equalTo(0)));
        assertThat(body.getHits(), is(empty()));
        assertDoesNotThrow(() -> body.getId().normalize());
    }

    @Test
    void handlerThrowsExceptionWhenGatewayIsBad() throws IOException {
        var elasticSearchClient = new ElasticSearchHighLevelRestClient(setUpBadGateWay());
        var handler = new SearchResourcesApiHandler(environment, elasticSearchClient);
        Executable executable = () -> handler.processInput(null, getRequestInfo(), mock(Context.class));
        assertThrows(ApiGatewayException.class, executable);
    }

    private RequestInfo getRequestInfo() {
        var requestInfo = new RequestInfo();
        requestInfo.setQueryParameters(Map.of(RequestUtil.SEARCH_TERM_KEY, SAMPLE_SEARCH_TERM));
        return requestInfo;
    }

    private RestHighLevelClientWrapper setUpRestHighLevelClient() throws IOException {
        String result = stringFromResources(Path.of(SAMPLE_ELASTICSEARCH_RESPONSE_JSON));
        SearchResponse searchResponse = getSearchResponse(result);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        when(restHighLevelClient.search(any(), any())).thenReturn(searchResponse);
        return new RestHighLevelClientWrapper(restHighLevelClient);
    }

    private RestHighLevelClientWrapper setUpRestHighLevelClientWithEmptyResponse() throws IOException {
        String result = stringFromResources(Path.of(EMPTY_ELASTICSEARCH_RESPONSE_JSON));
        SearchResponse searchResponse = getSearchResponse(result);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        when(restHighLevelClient.search(any(), any())).thenReturn(searchResponse);
        return new RestHighLevelClientWrapper(restHighLevelClient);
    }

    private RestHighLevelClientWrapper setUpBadGateWay() throws IOException {
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        when(restHighLevelClient.search(any(), any())).thenThrow(IOException.class);
        return new RestHighLevelClientWrapper(restHighLevelClient);
    }

    private SearchResponse getSearchResponse(String hits) {
        var searchResponse = mock(SearchResponse.class);
        when(searchResponse.toString()).thenReturn(hits);
        return searchResponse;
    }
}
