package no.unit.nva.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.moznion.uribuildertiny.URIBuilderTiny;
import no.unit.nva.search.exception.SearchException;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.utils.Environment;
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

    public static final String INITIAL_LOG_MESSAGE = "using Elasticsearch endpoint {} {} and index {}";
    public static final String SEARCHING_LOG_MESSAGE = "searching search index {}  for term {}";
    public static final String SOURCE_JSON_POINTER = "/_source";
    public static final String HITS_JSON_POINTER = "/hits/hits";
    public static final String ELASTICSEARCH_ENDPOINT_INDEX_KEY = "ELASTICSEARCH_ENDPOINT_INDEX";
    public static final String ELASTICSEARCH_ENDPOINT_ADDRESS_KEY = "ELASTICSEARCH_ENDPOINT_ADDRESS";
    public static final String ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY = "ELASTICSEARCH_ENDPOINT_API_SCHEME";

    public static final String ELASTIC_SEARCH_OPERATION = "_search";
    public static final String ELASTIC_QUERY_PARAMETER = "q";
    public static final String ELASTIC_SIZE_PARAMETER = "size";

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestClient.class);
    private static final ObjectMapper mapper = JsonUtils.objectMapper;

    private final HttpClient client;
    private final String elasticSearchEndpointAddress;
    private final String elasticSearchEndpointIndex;
    private final String elasticSearchEndpointScheme;

    /**
     * Creates a new ElasticSearchRestClient.
     *
     * @param httpClient  Client to speak http
     * @param environment Environment with properties
     */
    public ElasticSearchRestClient(HttpClient httpClient, Environment environment) {
        client = httpClient;
        elasticSearchEndpointAddress = environment.readEnv(ELASTICSEARCH_ENDPOINT_ADDRESS_KEY);
        elasticSearchEndpointIndex = environment.readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY);
        elasticSearchEndpointScheme = environment.readEnv(ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY);

        logger.info(INITIAL_LOG_MESSAGE,
                elasticSearchEndpointScheme, elasticSearchEndpointAddress, elasticSearchEndpointIndex);
    }

    /**
     * Searches for an term or index:term in elasticsearch index.
     *
     * @param term    search argument
     * @param results number of results
     * @throws ApiGatewayException thrown when uri is misconfigured, service i not available or interrupted
     */
    public SearchResourcesResponse searchSingleTerm(String term, String results) throws ApiGatewayException {

        try {
            HttpRequest request = createHttpRequest(term, results);
            HttpResponse<String> response = doSend(request);
            logger.debug(response.body());
            return toSearchResourcesResponse(response.body());
        } catch (IOException | InterruptedException e) {
            throw new SearchException(e.getMessage(), e);
        }
    }

    private SearchResourcesResponse toSearchResourcesResponse(String body) throws JsonProcessingException {
        JsonNode values = mapper.readTree(body);
        List<JsonNode> sourceList = extractSourceList(values);
        return SearchResourcesResponse.of(sourceList);
    }


    private HttpRequest createHttpRequest(String term, String results) {
        HttpRequest request = buildHttpRequest(term, results);
        logger.debug(SEARCHING_LOG_MESSAGE, elasticSearchEndpointIndex, term);
        return request;
    }

    private HttpRequest buildHttpRequest(String term, String results) {
        return HttpRequest.newBuilder()
                .uri(createSearchURI(term, results))
                .header(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType())
                .GET()
                .build();
    }


    private HttpResponse<String> doSend(HttpRequest request) throws IOException, InterruptedException {
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private URI createSearchURI(String term, String results) {
        return new URIBuilderTiny()
                .setScheme(elasticSearchEndpointScheme)
                .setHost(elasticSearchEndpointAddress)
                .setPaths(elasticSearchEndpointIndex, ELASTIC_SEARCH_OPERATION)
                .addQueryParameter(ELASTIC_QUERY_PARAMETER, term)
                .addQueryParameter(ELASTIC_SIZE_PARAMETER, results)
                .build();
    }

    private List<JsonNode> extractSourceList(JsonNode record) {
        return toStream(record.at(HITS_JSON_POINTER))
                .map(this::extractSourceStripped)
                .collect(Collectors.toList());
    }


    private JsonNode extractSourceStripped(JsonNode record) {
        return record.at(SOURCE_JSON_POINTER);
    }

    private Stream<JsonNode> toStream(JsonNode node) {
        return StreamSupport.stream(node.spliterator(), false);
    }

}
