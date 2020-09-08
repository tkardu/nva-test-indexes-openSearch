package no.unit.nva.search;

import no.unit.nva.search.exception.SearchException;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.utils.Environment;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ElasticSearchRestClient {


    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestClient.class);

    public static final String INITIAL_LOG_MESSAGE = "using Elasticsearch endpoint {} {} and index {}";
    public static final String SEARCHING_LOG_MESSAGE = "searching search index {}  for term {}";
    public static final String SEARCH_SINGLE_TERM_LOG_MESSAGE = "searching for term {}";

    private final HttpClient client;
    private final String elasticSearchEndpointAddress;
    private final String elasticSearchEndpointIndex;
    private final String elasticSearchEndpointScheme;

    /**
     * Creates a new ElasticSearchRestClient.
     *
     * @param httpClient Client to speak http
     * @param environment Environment with properties
     */
    public ElasticSearchRestClient(HttpClient httpClient, Environment environment) {
        client = httpClient;
        elasticSearchEndpointAddress = environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY);
        elasticSearchEndpointIndex = environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_INDEX_KEY);
        elasticSearchEndpointScheme = environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY);

        logger.info(INITIAL_LOG_MESSAGE,
                elasticSearchEndpointScheme, elasticSearchEndpointAddress, elasticSearchEndpointIndex);
    }

    /**
     * Searches for an term or index:term in elasticsearch index.
     * @param term search argument
     * @throws ApiGatewayException thrown when uri is misconfigured, service i not available or interrupted
     */
    public SearchResourcesResponse searchSingleTerm(String term) throws ApiGatewayException {

        HttpRequest request = null;
        try {
            request = createHttpRequest(term);
            HttpResponse<String> response = doSend(request);
            logger.debug(response.body());
            return toSearchResourcesResponse(response.body());
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new SearchException(e.getMessage(), e);
        }
    }

    private SearchResourcesResponse toSearchResourcesResponse(String body) {
        SearchResourcesResponse searchResourcesResponse = new SearchResourcesResponse();
        logger.debug("Body={}", body);
        return searchResourcesResponse;
    }


    private HttpRequest createHttpRequest(String term) throws URISyntaxException {

        HttpRequest request = buildHttpRequest(term);

        logger.debug(SEARCHING_LOG_MESSAGE, elasticSearchEndpointIndex, term);
        return request;
    }

    private HttpRequest buildHttpRequest(String term) throws URISyntaxException {
        return HttpRequest.newBuilder()
                .uri(createSearchURI(term))
                .header(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType())
                .GET()
                .build();
    }


    private HttpResponse<String> doSend(HttpRequest request) throws IOException, InterruptedException {
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private URI createSearchURI(String term) {
        String uriString = String.format(Constants.ELASTICSEARCH_SEARCH_ENDPOINT_URI_TEMPLATE,
                elasticSearchEndpointScheme, elasticSearchEndpointAddress,
                elasticSearchEndpointIndex, term);
        logger.debug("uriString={}",uriString);
        return URI.create(uriString);
    }

}
