package no.unit.nva.search;

import no.unit.nva.search.exception.SearchException;
import nva.commons.utils.Environment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.http.HttpClient;

import static no.unit.nva.search.ElasticSearchRestClient.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY;
import static no.unit.nva.search.ElasticSearchRestClient.ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY;
import static no.unit.nva.search.ElasticSearchRestClient.ELASTICSEARCH_ENDPOINT_INDEX_KEY;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticSearchRestClientTest {

    public static final String SAMPLE_TERM = "SampleSearchTerm";
    private static final String SAMPLE_NUMBER_OF_RESULTS = "7";
    private ElasticSearchHighLevelRestClient elasticSearchRestClient;
    private Environment environment;
    private HttpClient httpClient;

    private void initEnvironment() {
        when(environment.readEnv(ELASTICSEARCH_ENDPOINT_ADDRESS_KEY)).thenReturn("localhost");
        when(environment.readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY)).thenReturn("resources");
        when(environment.readEnv(ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY)).thenReturn("http");
    }

    /**
     * Set up test environment.
     **/
    @BeforeEach
    public void init() {
        environment = mock(Environment.class);
        httpClient = mock(HttpClient.class);
        initEnvironment();
        elasticSearchRestClient = new ElasticSearchHighLevelRestClient(environment);
    }

    @Test
    public void defaultConstructorWithEnvironmentIsNullShouldFail() {
        assertThrows(NullPointerException.class, () -> new ElasticSearchHighLevelRestClient(null));
    }

    @Test
    public void constructorWithEnvironmentDefinedShouldCreateInstance() {
        initEnvironment();
        ElasticSearchHighLevelRestClient elasticSearchRestClient = new ElasticSearchHighLevelRestClient(environment);
        assertNotNull(elasticSearchRestClient);
    }

    @Test
    public void callingSearchSingleTermHandlesExceptionInHttpClient() throws IOException, InterruptedException {
        doThrow(IOException.class).when(httpClient).send(any(), any());
        assertThrows(SearchException.class, () -> elasticSearchRestClient.searchSingleTerm(SAMPLE_TERM,
                SAMPLE_NUMBER_OF_RESULTS));
    }

}
