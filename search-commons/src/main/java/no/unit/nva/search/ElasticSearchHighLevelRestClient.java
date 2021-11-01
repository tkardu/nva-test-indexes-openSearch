package no.unit.nva.search;

import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_ENDPOINT_ADDRESS;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_ENDPOINT_INDEX;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_REGION;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTIC_SEARCH_SERVICE_NAME;
import static no.unit.nva.search.constants.ApplicationConstants.PUBLICATION_API_BASE_ADDRESS;
import static no.unit.nva.search.constants.ApplicationConstants.SEARCH_API_BASE_ADDRESS;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapper;
import static nva.commons.core.attempt.Try.attempt;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import no.unit.nva.model.Publication;
import no.unit.nva.search.exception.SearchException;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.core.attempt.Try;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchHighLevelRestClient {

    public static final String INITIAL_LOG_MESSAGE = "using Elasticsearch endpoint {} and index {}";
    public static final String SOURCE_JSON_POINTER = "/_source";
    public static final String TOTAL_JSON_POINTER = "/hits/total/value";
    public static final String TOOK_JSON_POINTER = "/took";
    public static final String HITS_JSON_POINTER = "/hits/hits";
    public static final String DOCUMENT_WITH_ID_WAS_NOT_FOUND_IN_ELASTICSEARCH
            = "Document with id={} was not found in elasticsearch";
    public static final URI DEFAULT_SEARCH_CONTEXT = URI.create("https://api.nva.unit.no/resources/search");
    public static final String ELASTIC_SEARCH_NUMBER_OF_REPLICAS = "index.number_of_replicas";
    public static final int BULK_SIZE = 100;
    public static final boolean SEQUENTIAL = false;
    public static final String QUERY_PARAMETER_START = "?query=";
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchHighLevelRestClient.class);
    private static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
    private final RestHighLevelClientWrapper elasticSearchClient;

    /**
     * Creates a new ElasticSearchRestClient.
     */
    public ElasticSearchHighLevelRestClient() {
        elasticSearchClient = createElasticsearchClientWithInterceptor();
        logger.info(INITIAL_LOG_MESSAGE, ELASTICSEARCH_ENDPOINT_ADDRESS, ELASTICSEARCH_ENDPOINT_INDEX);
    }

    /**
     * Creates a new ElasticSearchRestClient.
     *
     * @param elasticSearchClient client to use for access to ElasticSearch
     */
    public ElasticSearchHighLevelRestClient(RestHighLevelClientWrapper elasticSearchClient) {

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
        try {
            SearchResponse searchResponse = doSearch(term, results, from, orderBy, sortOrder);
            return toSearchResourcesResponse(term, searchResponse.toString());
        } catch (Exception e) {
            throw new SearchException(e.getMessage(), e);
        }
    }

    /**
     * Adds or insert a document to an elasticsearch index.
     *
     * @param document the document to be inserted
     * @throws SearchException when something goes wrong
     */
    public void addDocumentToIndex(IndexDocument document) throws SearchException {
        try {
            doUpsert(getUpdateRequest(document));
        } catch (Exception e) {
            throw new SearchException(e.getMessage(), e);
        }
    }

    public void addDocumentToIndex(IndexRequest indexRequest) throws SearchException {
        try {
            doUpsert(indexRequest);
        } catch (Exception e) {
            throw new SearchException(e.getMessage(), e);
        }
    }

    /**
     * Removes an document from Elasticsearch index.
     *
     * @param identifier og document
     * @throws SearchException when
     */
    public void removeDocumentFromIndex(String identifier) throws SearchException {
        try {
            doDelete(identifier);
        } catch (Exception e) {
            throw new SearchException(e.getMessage(), e);
        }
    }


    public Stream<BulkResponse> batchInsert(Stream<Publication> publications) {
        Stream<List<Publication>> stream = splitStreamToBatches(publications);

        return stream.map(attempt(this::insertBatch)).map(Try::orElseThrow);
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

    private Stream<List<Publication>> splitStreamToBatches(Stream<Publication> indexDocuments) {
        UnmodifiableIterator<List<Publication>> bulks = Iterators.partition(
                indexDocuments.iterator(), BULK_SIZE);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(bulks, Spliterator.ORDERED), SEQUENTIAL);
    }

    private BulkResponse insertBatch(List<Publication> bulk) throws IOException {
        List<IndexRequest> indexRequests = bulk.stream()
            .parallel()
            .map(IndexDocument::fromPublication)
            .map(this::getUpdateRequest)
            .collect(Collectors.toList());

        BulkRequest request = new BulkRequest();
        indexRequests.forEach(request::add);
        request.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        request.waitForActiveShards(ActiveShardCount.ONE);
        return elasticSearchClient.bulk(request, RequestOptions.DEFAULT);
    }

    private SearchResponse doSearch(String term,
                                    int results,
                                    int from,
                                    String orderBy,
                                    SortOrder sortOrder) throws IOException {
        return elasticSearchClient.search(getSearchRequest(term,
                results,
                from,
                orderBy,
                sortOrder), RequestOptions.DEFAULT);
    }

    private SearchRequest getSearchRequest(String term, int results, int from, String orderBy, SortOrder sortOrder) {
        final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.queryStringQuery(term))
                .sort(orderBy, sortOrder)
                .from(from)
                .size(results);
        return new SearchRequest(ELASTICSEARCH_ENDPOINT_INDEX).source(sourceBuilder);
    }

    private void doUpsert(IndexRequest request) throws IOException {
        elasticSearchClient.index(request, RequestOptions.DEFAULT);
    }

    private IndexRequest getUpdateRequest(IndexDocument document) {
        return new IndexRequest(ELASTICSEARCH_ENDPOINT_INDEX)
                .source(document.toJsonString(), XContentType.JSON)
                .id(document.getIdentifier().toString());
    }

    private void doDelete(String identifier) throws IOException {
        DeleteResponse deleteResponse = elasticSearchClient
                .delete(new DeleteRequest(ELASTICSEARCH_ENDPOINT_INDEX, identifier),
                        RequestOptions.DEFAULT);
        if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            logger.warn(DOCUMENT_WITH_ID_WAS_NOT_FOUND_IN_ELASTICSEARCH, identifier);
        }
    }

    private SearchResourcesResponse toSearchResourcesResponse(String searchterm, String body)
            throws JsonProcessingException {
        JsonNode values = objectMapper.readTree(body);

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
                .map(this::extractIdAndContext)
                .collect(Collectors.toList());
    }

    private JsonNode extractSourceStripped(JsonNode record) {
        return record.at(SOURCE_JSON_POINTER);
    }

    private JsonNode extractIdAndContext(JsonNode record) {
        if (recordHasNoId(record)) {
            ((ObjectNode) record).put(IndexDocument.ID_FIELD_NAME, createId(record));
        }
        return record;
    }

    private String createId(JsonNode record) {
        String identifier = record.at(IndexDocument.IDENTIFIER_JSON_PTR).textValue();
        return IndexDocument.mergeStringsWithDelimiter(PUBLICATION_API_BASE_ADDRESS, identifier);
    }

    private boolean recordHasNoId(JsonNode record) {
        return !record.has(IndexDocument.ID_FIELD_NAME);
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
