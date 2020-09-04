package no.unit.nva.search;

import no.unit.nva.search.exception.SearchException;
import nva.commons.utils.Environment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticSearchRestClientTest {

    public static final String SAMPLE_TERM = "SampleSearchTerm";
    ElasticSearchRestClient elasticSearchRestClient;
    private Environment environment;
    private HttpClient httpClient;


    private void initEnvironment() {
        when(environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY)).thenReturn("localhost");
        when(environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_INDEX_KEY)).thenReturn("resources");
        when(environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY)).thenReturn("http");
    }

    /**
     * Set up test environment.
     **/
    @BeforeEach
    public void init() {
        environment = mock(Environment.class);
        httpClient = mock(HttpClient.class);
        initEnvironment();
        elasticSearchRestClient = new ElasticSearchRestClient(httpClient, environment);
    }

    @Test
    public void defaultConstructorWithEnvironmentIsNullShouldFail() {
        assertThrows(NullPointerException.class, () -> new ElasticSearchRestClient(httpClient, null));
    }

    @Test
    public void constructorWithEnvironmentDefinedShouldCreateInstance() {
        initEnvironment();
        ElasticSearchRestClient elasticSearchRestClient = new ElasticSearchRestClient(httpClient, environment);
        assertNotNull(elasticSearchRestClient);
    }

    @Test
    public void callingSearchSingleTermProducesAnAnswer() throws Exception {
        HttpResponse<String> successResponse = mock(HttpResponse.class);
        doReturn(successResponse).when(httpClient).send(any(), any());

        SearchResourcesResponse searchResourcesResponse = elasticSearchRestClient.searchSingleTerm(SAMPLE_TERM);
        assertNotNull(searchResourcesResponse);
    }

    @Test
    public void callingSearchSingleTermHandlesExceptionInHttpClient() throws IOException, InterruptedException {
        doThrow(IOException.class).when(httpClient).send(any(), any());
        assertThrows(SearchException.class, () -> elasticSearchRestClient.searchSingleTerm(SAMPLE_TERM));
    }

}
