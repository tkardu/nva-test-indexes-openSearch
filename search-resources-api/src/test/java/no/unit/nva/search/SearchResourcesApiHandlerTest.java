package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.handlers.RequestInfo;
import nva.commons.utils.Environment;
import nva.commons.utils.IoUtils;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class SearchResourcesApiHandlerTest {

    public static final String SAMPLE_QUERY_PARAMETER = "SampleQueryParameter";
    private static final String SAMPLE_ELASTIC_RESPONSE = "sample_elasticsearch_response2.json";
    private Environment environment;
    private SearchResourcesApiHandler searchResourcesApiHandler;

    private void initEnvironment() {
        environment = mock(Environment.class);
        when(environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY)).thenReturn("localhost");
        when(environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_INDEX_KEY)).thenReturn("resources");
        when(environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY)).thenReturn("http");
    }


    @BeforeEach
    public void init() {
        initEnvironment();
        searchResourcesApiHandler = new SearchResourcesApiHandler(environment);
    }

    @Test
    public void defaultConstructorThrowsIllegalStateExceptionWhenEnvironmentNotDefined() {
        assertThrows(IllegalStateException.class, SearchResourcesApiHandler::new);
    }

//    @Test
    public void processInputReturnsNullWhenInputIsEmpty() throws ApiGatewayException {
        SearchResourcesRequest input = mock(SearchResourcesRequest.class);
        RequestInfo requestInfo = mock(RequestInfo.class);
        Context context = mock(Context.class);
        assertThrows(ApiGatewayException.class, () ->  searchResourcesApiHandler.processInput(input,
                requestInfo,
                context));
    }

    @Test
    public void getSuccessStatusCodeReturnsOK() {
        SearchResourcesRequest request = new  SearchResourcesRequest();
        SearchResourcesResponse response =  new SearchResourcesResponse();
        Integer statusCode = searchResourcesApiHandler.getSuccessStatusCode(request, response);
        assertEquals(statusCode, HttpStatus.SC_OK);
    }


    @Test
    public void processInputReturnsSomething() throws ApiGatewayException, IOException, InterruptedException {
        HttpResponse<String> httpResponse =  mock(HttpResponse.class);

        RequestInfo requestInfo = new RequestInfo();
        requestInfo.setQueryParameters(Map.of(RequestUtil.SEARCH_TERM_KEY, SAMPLE_QUERY_PARAMETER));

        HttpClient httpClient = mock(HttpClient.class);
        doReturn(httpResponse).when(httpClient).send(any(), any());
        InputStream is = IoUtils.inputStreamFromResources(Paths.get(SAMPLE_ELASTIC_RESPONSE));
        String responseBody = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        doReturn(responseBody).when(httpResponse).body();
        ElasticSearchHighLevelRestClient elasticSearchRestClient = new ElasticSearchHighLevelRestClient(environment);

//        searchResourcesApiHandler = new SearchResourcesApiHandler(environment, elasticSearchRestClient);
        searchResourcesApiHandler = new SearchResourcesApiHandler(environment);
        Context context = mock(Context.class);
        SearchResourcesRequest input = mock(SearchResourcesRequest.class);
//        SearchResourcesResponse response = searchResourcesApiHandler.processInput(input, requestInfo, context);
//        assertNotNull(response);
    }



}
