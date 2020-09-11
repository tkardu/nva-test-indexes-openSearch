package no.unit.nva.search;

import nva.commons.exceptions.ApiGatewayException;
import nva.commons.utils.Environment;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticsearchSigningHighLevelRestClientTest {


    private static final String elasticSearchEndpoint = "https://search-sg-elas-nvaela-l01tk4es8umt-26a2ccz2exi6xkxqg46y7ry63q.eu-west-1.es.amazonaws.com"; // e.g. https://search-mydomain.us-west-1.es.amazonaws.com
    public static final String SAMPLE_TERM = "SampleSearchTerm";
    private static final String SAMPLE_NUMBER_OF_RESULTS = "7";
    ElasticSearchHighLevelRestClient elasticSearchRestClient;
    private Environment environment;

    private void initEnvironment() {
        when(environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY)).thenReturn(elasticSearchEndpoint);
        when(environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_INDEX_KEY)).thenReturn("resources");
        when(environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY)).thenReturn("https");
    }

    /**
     * Set up test environment.
     **/
    @BeforeEach
    public void init() {
        environment = mock(Environment.class);
        initEnvironment();
        elasticSearchRestClient = new ElasticSearchHighLevelRestClient(environment);
    }


//    @Test
    public void callingSearchSingleTermHandlesExceptionInHttpClient() throws ApiGatewayException {
        SearchResourcesResponse searchResourcesResponse =
                elasticSearchRestClient.searchSingleTerm(SAMPLE_TERM, SAMPLE_NUMBER_OF_RESULTS);
        assertNotNull(searchResourcesResponse);
   }


}
