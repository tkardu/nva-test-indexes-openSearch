package no.unit.nva.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.search.exception.SearchException;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.utils.Environment;
import nva.commons.utils.JacocoGenerated;
import nva.commons.utils.JsonUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ElasticSearchRestClient {


    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestClient.class);

    public static final String INITIAL_LOG_MESSAGE = "using Elasticsearch endpoint {} {} and index {}";
    public static final String SEARCHING_LOG_MESSAGE = "searching search index {}  for term {}";

    private static final ObjectMapper mapper = JsonUtils.objectMapper;
    public static final String SOURCE_JSON_POINTER = "/_source";
    public static final String HITS_JSON_POINTER = "/hits/hits";
    public static final String ERROR_READING_RESPONSE_FROM_ELASTIC_SEARCH =
            "Error when reading response from ElasticSearch";

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

        try {
            HttpRequest request = createHttpRequest(term);
            HttpResponse<String> response = doSend(request);
            logger.debug(response.body());
            return toSearchResourcesResponse(response.body());
        } catch (IOException | InterruptedException e) {
            throw new SearchException(e.getMessage(), e);
        }
    }

    private SearchResourcesResponse toSearchResourcesResponse(String body) throws JsonProcessingException {
        JsonNode values = mapper.readTree(body);
        List<String> sourceList = extractSourceList(values);
        return SearchResourcesResponse.of(sourceList);
    }


    private HttpRequest createHttpRequest(String term) {

        HttpRequest request = buildHttpRequest(term);

        logger.debug(SEARCHING_LOG_MESSAGE, elasticSearchEndpointIndex, term);
        return request;
    }

    private HttpRequest buildHttpRequest(String term) {
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

    private List<String> extractSourceList(JsonNode record) {
        return toStream(record.at(HITS_JSON_POINTER))
                .map(this::extractSourceStripped)
                .collect(Collectors.toList());
    }

    @JacocoGenerated
    private String extractSourceStripped(JsonNode record) {
        JsonNode jsonNode = record.at(SOURCE_JSON_POINTER);
        return jsonNode.toString();
    }


    private Stream<JsonNode> toStream(JsonNode contributors) {
        return StreamSupport.stream(contributors.spliterator(), false);
    }

}
