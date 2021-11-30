package no.unit.nva.search;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.fasterxml.jackson.databind.JsonNode;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.apigateway.exceptions.BadGatewayException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static no.unit.nva.search.IndexedDocumentsJsonPointers.SOURCE_JSON_POINTER;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_ENDPOINT_ADDRESS;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_ENDPOINT_INDEX;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_REGION;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTIC_SEARCH_SERVICE_NAME;
import static no.unit.nva.search.constants.ApplicationConstants.SEARCH_API_BASE_ADDRESS;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static nva.commons.core.attempt.Try.attempt;

public class SearchClient {

    public static final String INITIAL_LOG_MESSAGE = "using Elasticsearch endpoint {} and index {}";

    public static final String TOTAL_JSON_POINTER = "/hits/total/value";
    public static final String TOOK_JSON_POINTER = "/took";
    public static final String HITS_JSON_POINTER = "/hits/hits";
    public static final String DOCUMENT_WITH_ID_WAS_NOT_FOUND_IN_ELASTICSEARCH
            = "Document with id={} was not found in elasticsearch";
    public static final URI DEFAULT_SEARCH_CONTEXT = URI.create("https://api.nva.unit.no/resources/search");
    public static final int BULK_SIZE = 100;
    public static final boolean SEQUENTIAL = false;
    public static final String QUERY_PARAMETER_START = "?query=";
    private static final Logger logger = LoggerFactory.getLogger(SearchClient.class);
    private static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
    public static final String NO_RESPONSE_FROM_INDEX = "No response from index";
    public static final String UNABLE_TO_PARSE_SEARCH_RESPONSE = "Unable to parse SearchResponse";
    private final RestHighLevelClientWrapper elasticSearchClient;

    /**
     * Creates a new ElasticSearchRestClient.
     */
    public SearchClient() {
        elasticSearchClient = createElasticsearchClientWithInterceptor();
        logger.info(INITIAL_LOG_MESSAGE, ELASTICSEARCH_ENDPOINT_ADDRESS, ELASTICSEARCH_ENDPOINT_INDEX);
    }

    /**
     * Creates a new ElasticSearchRestClient.
     *
     * @param elasticSearchClient client to use for access to ElasticSearch
     */
    public SearchClient(RestHighLevelClientWrapper elasticSearchClient) {

        this.elasticSearchClient = elasticSearchClient;
        logger.info(INITIAL_LOG_MESSAGE, ELASTICSEARCH_ENDPOINT_ADDRESS, ELASTICSEARCH_ENDPOINT_INDEX);
    }

    private static int intFromNode(JsonNode jsonNode, String jsonPointer) {
        JsonNode json = jsonNode.at(jsonPointer);
        return isPopulated(json) ? json.asInt() : 0;
    }

    private static boolean isPopulated(JsonNode json) {
        return !json.isNull() && !json.asText().isBlank();
    }

    /**
     * Searches for an term or index:term in elasticsearch index.
     *
     * @param term    search argument
     * @param results number of results
     * @throws ApiGatewayException thrown when uri is misconfigured, service i not available or interrupted
     */
    public SearchResourcesResponse searchSingleTerm(String term,
                                                    int results,
                                                    int from,
                                                    String orderBy,
                                                    SortOrder sortOrder) throws ApiGatewayException {
        SearchResponse searchResponse = doSearch(term, results, from, orderBy, sortOrder);
        return toSearchResourcesResponse(term, searchResponse.toString());
    }




    protected final RestHighLevelClientWrapper createElasticsearchClientWithInterceptor() {
        AWS4Signer signer = getAws4Signer();
        HttpRequestInterceptor interceptor =
                new AWSRequestSigningApacheInterceptor(ELASTIC_SEARCH_SERVICE_NAME,
                        signer,
                        credentialsProvider);

        RestClientBuilder clientBuilder = RestClient
                .builder(HttpHost.create(ELASTICSEARCH_ENDPOINT_ADDRESS))
                .setHttpClientConfigCallback(config -> config.addInterceptorLast(interceptor));
        return new RestHighLevelClientWrapper(clientBuilder);
    }



    private SearchResponse doSearch(String term,
                                    int results,
                                    int from,
                                    String orderBy,
                                    SortOrder sortOrder) throws BadGatewayException {
        try {
            return elasticSearchClient.search(getSearchRequest(term,
                    results,
                    from,
                    orderBy,
                    sortOrder), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new BadGatewayException(NO_RESPONSE_FROM_INDEX);
        }

    }

    private SearchRequest getSearchRequest(String term, int results, int from, String orderBy, SortOrder sortOrder) {
        final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.queryStringQuery(term))
                .sort(orderBy, sortOrder)
                .from(from)
                .size(results);
        return new SearchRequest(ELASTICSEARCH_ENDPOINT_INDEX).source(sourceBuilder);
    }


    private SearchResourcesResponse toSearchResourcesResponse(String searchterm, String body) {

        JsonNode values = attempt(() -> objectMapperWithEmpty.readTree(body)).orElseThrow();

        List<JsonNode> sourceList = extractSourceList(values);
        int total = intFromNode(values, TOTAL_JSON_POINTER);
        int took = intFromNode(values, TOOK_JSON_POINTER);
        URI searchResultId =
                URI.create(createIdWithQuery(searchterm));
        return new SearchResourcesResponse.Builder()
                .withContext(DEFAULT_SEARCH_CONTEXT)
                .withId(searchResultId)
                .withTook(took)
                .withTotal(total)
                .withHits(sourceList)
                .build();
    }

    private String createIdWithQuery(String searchterm) {
        return SEARCH_API_BASE_ADDRESS + QUERY_PARAMETER_START +  URLEncoder.encode(searchterm, StandardCharsets.UTF_8);
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

    private AWS4Signer getAws4Signer() {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(ELASTIC_SEARCH_SERVICE_NAME);
        signer.setRegionName(ELASTICSEARCH_REGION);
        return signer;
    }
}
