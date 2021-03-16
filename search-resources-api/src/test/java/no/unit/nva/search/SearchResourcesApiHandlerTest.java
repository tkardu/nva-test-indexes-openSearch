package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_INDEX_KEY;
import static nva.commons.apigateway.ApiGatewayHandler.ALLOWED_ORIGIN_ENV;
import static nva.commons.core.ioutils.IoUtils.stringFromResources;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
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
    public static final ObjectMapper mapper = JsonUtils.objectMapperWithEmpty;
    public static final String ROUNDTRIP_RESPONSE_JSON = "roundtripResponse.json";
    public static final String EMPTY_ROUNDTRIP_RESPONSE_JSON = "empty_roundtripResponse.json";
    public static final URI EXAMPLE_CONTEXT = URI.create("https://example.org/search");
    public static final List<JsonNode> SAMPLE_HITS = Collections.EMPTY_LIST;
    public static final int SAMPLE_TOOK = 0;
    public static final int SAMPLE_TOTAL = 0;
    private static String ZERO_TOTAL = " \"total\" : 0";
    private static String EMPTY_HITS = "\"hits\" : [ ]";
    private Environment environment;
    private SearchResourcesApiHandler searchResourcesApiHandler;

    private void initEnvironment() {
        environment = mock(Environment.class);
        when(environment.readEnv(ELASTICSEARCH_ENDPOINT_ADDRESS_KEY)).thenReturn("localhost");
        when(environment.readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY)).thenReturn("resources");
        when(environment.readEnv(ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY)).thenReturn("http");
        when(environment.readEnv(ALLOWED_ORIGIN_ENV)).thenReturn("*");
    }

    @BeforeEach
    void init() {
        initEnvironment();
        searchResourcesApiHandler = new SearchResourcesApiHandler(environment);
    }

    @Test
    void defaultConstructorThrowsIllegalStateExceptionWhenEnvironmentNotDefined() {
        assertThrows(IllegalStateException.class, SearchResourcesApiHandler::new);
    }

    @Test
    void getSuccessStatusCodeReturnsOK() {
        SearchResourcesResponse response =  new SearchResourcesResponse(EXAMPLE_CONTEXT,
                SAMPLE_TOOK,
                SAMPLE_TOTAL,
                SAMPLE_HITS);
        Integer statusCode = searchResourcesApiHandler.getSuccessStatusCode(null, response);
        assertEquals(statusCode, HttpStatus.SC_OK);
    }

    @Test
    void handlerReturnsSearchResultsWhenQueryIsSingleTerm() throws ApiGatewayException, IOException {
        var elasticSearchClient = new ElasticSearchHighLevelRestClient(environment, setUpRestHighLevelClient());
        var handler = new SearchResourcesApiHandler(environment, elasticSearchClient);
        var actual = handler.processInput(null, getRequestInfo(), mock(Context.class));
        var expected = mapper.readValue(stringFromResources(Path.of(ROUNDTRIP_RESPONSE_JSON)),
                SearchResourcesResponse.class);
        assertEquals(expected, actual);
    }

    @Test
    void handlerReturnsSearchResultsWithEmptyHistsWhenQueryResultIsEmpty() throws ApiGatewayException, IOException {
        var elasticSearchClient =
                new ElasticSearchHighLevelRestClient(environment, setUpRestHighLevelClientWithEmptyResponse());
        var handler = new SearchResourcesApiHandler(environment, elasticSearchClient);
        var inputStream = IoUtils.inputStreamFromResources(EMPTY_ELASTICSEARCH_RESPONSE_JSON);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        handler.handleRequest(inputStream, outputStream,  mock(Context.class));
        String string = new String(outputStream.toByteArray(), Charset.defaultCharset());
        GatewayResponseWithEmptyValues<SearchResourcesResponse> gatewayResponse
                = mapper.readValue(string, GatewayResponseWithEmptyValues.class);
        String body = gatewayResponse.getBody();

        assertNotNull(gatewayResponse.getHeaders());
        assertEquals(gatewayResponse.getStatusCode(), HttpStatus.SC_OK);
        assertThat(body, containsString(ZERO_TOTAL));
        assertThat(body, containsString(EMPTY_HITS));
    }

    @Test
    void handlerThrowsExceptionWhenGatewayIsBad() throws IOException {
        var elasticSearchClient = new ElasticSearchHighLevelRestClient(environment, setUpBadGateWay());
        var handler = new SearchResourcesApiHandler(environment, elasticSearchClient);
        Executable executable = () -> handler.processInput(null, getRequestInfo(), mock(Context.class));
        assertThrows(ApiGatewayException.class, executable);
    }

    private RequestInfo getRequestInfo() {
        var requestInfo = new RequestInfo();
        requestInfo.setQueryParameters(Map.of(RequestUtil.SEARCH_TERM_KEY, SAMPLE_SEARCH_TERM));
        return requestInfo;
    }

    private RestHighLevelClient setUpRestHighLevelClient() throws IOException {
        String result = stringFromResources(Path.of(SAMPLE_ELASTICSEARCH_RESPONSE_JSON));
        SearchResponse searchResponse = getSearchResponse(result);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        when(restHighLevelClient.search(any(), any())).thenReturn(searchResponse);
        return restHighLevelClient;
    }

    private RestHighLevelClient setUpRestHighLevelClientWithEmptyResponse() throws IOException {
        String result = stringFromResources(Path.of(EMPTY_ELASTICSEARCH_RESPONSE_JSON));
        SearchResponse searchResponse = getSearchResponse(result);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        when(restHighLevelClient.search(any(), any())).thenReturn(searchResponse);
        return restHighLevelClient;
    }


    private RestHighLevelClient setUpBadGateWay() throws IOException {
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        when(restHighLevelClient.search(any(), any())).thenThrow(IOException.class);
        return restHighLevelClient;
    }

    private SearchResponse getSearchResponse(String hits) {
        var searchResponse = mock(SearchResponse.class);
        when(searchResponse.toString()).thenReturn(hits);
        return searchResponse;
    }
}
