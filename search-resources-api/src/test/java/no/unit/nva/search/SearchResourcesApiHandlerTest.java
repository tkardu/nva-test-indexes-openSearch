package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import no.unit.nva.testutils.HandlerRequestBuilder;
import nva.commons.apigateway.GatewayResponse;
import nva.commons.core.Environment;
import nva.commons.core.ioutils.IoUtils;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zalando.problem.Problem;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchResourcesApiHandlerTest {

    public static final String SAMPLE_SEARCH_TERM = "searchTerm";
    public static final String SAMPLE_ELASTICSEARCH_RESPONSE_JSON = "sample_elasticsearch_response.json";
    public static final String EMPTY_ELASTICSEARCH_RESPONSE_JSON = "empty_elasticsearch_response.json";
    public static final String ROUNDTRIP_RESPONSE_JSON = "roundtripResponse.json";

    private RestHighLevelClient restHighLevelClientMock;
    private SearchResourcesApiHandler handler;
    private Context contextMock;
    private ByteArrayOutputStream outputStream;

    @BeforeEach
    void init() {
        restHighLevelClientMock = mock(RestHighLevelClient.class);
        RestHighLevelClientWrapper restHighLevelClientWrapper = new RestHighLevelClientWrapper(restHighLevelClientMock);
        SearchClient searchClient = new SearchClient(restHighLevelClientWrapper);
        handler = new SearchResourcesApiHandler(new Environment(), searchClient);
        contextMock = mock(Context.class);
        outputStream = new ByteArrayOutputStream();
    }

    @Test
    void shouldReturnSearchResultsWhenQueryIsSingleTerm() throws IOException {
        prepareRestHighLevelClientOkResponse();

        handler.handleRequest(getInputStream(), outputStream, contextMock);

        GatewayResponse<SearchResourcesResponse> gatewayResponse = GatewayResponse.fromOutputStream(outputStream);
        SearchResourcesResponse actual = gatewayResponse.getBodyObject(SearchResourcesResponse.class);

        SearchResourcesResponse expected = getSearchResourcesResponseFromFile(ROUNDTRIP_RESPONSE_JSON);

        assertNotNull(gatewayResponse.getHeaders());
        assertEquals(gatewayResponse.getStatusCode(), HttpStatus.SC_OK);
        assertThat(actual, is(equalTo(expected)));
    }

    @Test
    void shouldReturnSearchResultsWithEmptyHitsWhenQueryResultIsEmpty() throws IOException {
        prepareRestHighLevelClientEmptyResponse();

        var inputStream = IoUtils.inputStreamFromResources(EMPTY_ELASTICSEARCH_RESPONSE_JSON);

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
    void shouldReturnProblemResponseWhenGatewayIsBad() throws IOException {
        prepareRestHighLevelClientBadGateWayResponse();

        handler.handleRequest(getInputStream(), outputStream, mock(Context.class));

        GatewayResponse<Problem> gatewayResponse = GatewayResponse.fromOutputStream(outputStream);

        assertNotNull(gatewayResponse.getHeaders());
        assertEquals(gatewayResponse.getStatusCode(), HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }

    private InputStream getInputStream() throws JsonProcessingException {
        return new HandlerRequestBuilder<Void>(objectMapperWithEmpty)
                .withQueryParameters(Map.of(RequestUtil.SEARCH_TERM_KEY, SAMPLE_SEARCH_TERM))
                .build();
    }

    private void prepareRestHighLevelClientOkResponse() throws IOException {
        String result = stringFromResources(Path.of(SAMPLE_ELASTICSEARCH_RESPONSE_JSON));
        SearchResponse searchResponse = createSearchResponseWithHits(result);

        when(restHighLevelClientMock.search(any(), any())).thenReturn(searchResponse);
    }

    private void prepareRestHighLevelClientEmptyResponse() throws IOException {
        String result = stringFromResources(Path.of(EMPTY_ELASTICSEARCH_RESPONSE_JSON));
        SearchResponse searchResponse = createSearchResponseWithHits(result);

        when(restHighLevelClientMock.search(any(), any())).thenReturn(searchResponse);
    }

    private void prepareRestHighLevelClientBadGateWayResponse() throws IOException {
        when(restHighLevelClientMock.search(any(), any())).thenThrow(IOException.class);
    }

    private SearchResourcesResponse getSearchResourcesResponseFromFile(String filename)
            throws JsonProcessingException {
        return objectMapperWithEmpty
                .readValue(stringFromResources(Path.of(filename)), SearchResourcesResponse.class);
    }

    private SearchResponse createSearchResponseWithHits(String hits) {
        var searchResponse = mock(SearchResponse.class);
        when(searchResponse.toString()).thenReturn(hits);
        return searchResponse;
    }
}
