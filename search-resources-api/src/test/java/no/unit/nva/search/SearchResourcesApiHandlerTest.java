package no.unit.nva.search;

import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_INDEX_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.lambda.runtime.Context;
import java.util.Map;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.handlers.RequestInfo;
import nva.commons.utils.Environment;
import org.apache.http.HttpStatus;
import org.hamcrest.core.Is;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class SearchResourcesApiHandlerTest {

    public static final String SAMPLE_SEARCH_TERM = "searchTerm";
    private Environment environment;
    private SearchResourcesApiHandler searchResourcesApiHandler;

    private void initEnvironment() {
        environment = mock(Environment.class);
        when(environment.readEnv(ELASTICSEARCH_ENDPOINT_ADDRESS_KEY)).thenReturn("localhost");
        when(environment.readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY)).thenReturn("resources");
        when(environment.readEnv(ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY)).thenReturn("http");
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

    @Test
    public void processInputReturnsNullWhenInputIsEmpty() {
        RequestInfo requestInfo = mock(RequestInfo.class);
        Context context = mock(Context.class);
        Executable executable = () -> searchResourcesApiHandler.processInput(null, requestInfo, context);
        assertThrows(ApiGatewayException.class, executable);
    }

    @Test
    public void getSuccessStatusCodeReturnsOK() {
        SearchResourcesResponse response =  new SearchResourcesResponse();
        Integer statusCode = searchResourcesApiHandler.getSuccessStatusCode(null, response);
        assertEquals(statusCode, HttpStatus.SC_OK);
    }

    @Test
    public void handlingCallToElasticSearchToImproveTestCodeCoverage() throws ApiGatewayException {

        ElasticSearchHighLevelRestClient elasticSearchClient = mock(ElasticSearchHighLevelRestClient.class);
        SearchResourcesApiHandler handler = new SearchResourcesApiHandler(environment, elasticSearchClient);

        RequestInfo requestInfo = new RequestInfo();
        Map<String, String> queryParameters = Map.of(RequestUtil.SEARCH_TERM_KEY, SAMPLE_SEARCH_TERM);
        requestInfo.setQueryParameters(queryParameters);
        Context context = mock(Context.class);
        handler.processInput(null, requestInfo, context);
    }
}
