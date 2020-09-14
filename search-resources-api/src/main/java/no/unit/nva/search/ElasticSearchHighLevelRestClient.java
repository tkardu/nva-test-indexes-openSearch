package no.unit.nva.search;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.search.exception.SearchException;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.utils.Environment;
import nva.commons.utils.JacocoGenerated;
import nva.commons.utils.JsonUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@JacocoGenerated
public class ElasticSearchHighLevelRestClient {


    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchHighLevelRestClient.class);

    private static final String SERVICE_NAME = "es";
    public static final String ELASTICSEARCH_ENDPOINT_INDEX_KEY = "ELASTICSEARCH_ENDPOINT_INDEX";
    public static final String ELASTICSEARCH_ENDPOINT_ADDRESS_KEY = "ELASTICSEARCH_ENDPOINT_ADDRESS";
    public static final String ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY = "ELASTICSEARCH_ENDPOINT_API_SCHEME";
    public static final String ELASTICSEARCH_ENDPOINT_REGION_KEY = "ELASTICSEARCH_REGION";


    public static final String INITIAL_LOG_MESSAGE = "using Elasticsearch endpoint {} and index {}";
    public static final String UPSERTING_LOG_MESSAGE = "Upserting search index  with values {}";
    public static final String DELETE_LOG_MESSAGE = "Deleting from search API publication with identifier: {}";
    public static final String SOURCE_JSON_POINTER = "/_source";
    public static final String HITS_JSON_POINTER = "/hits/hits";
    public static final String ADD_DOCUMENT_LOG_MESSAGE = "elasticSearchEndpointIndex= {}, document.getIdentifier()={}";
    public static final String DOCUMENT_WITH_ID_WAS_NOT_FOUND_IN_ELASTICSEARCH
            = "Document with id={} was not found in elasticsearch";

    private static final ObjectMapper mapper = JsonUtils.objectMapper;
    public static final String SEARCH_REQUEST_MESSAGE = "Searching index={} for term={}, searchRequest={}";
    private final String elasticSearchEndpointAddress;


    private final String elasticSearchRegion;
    private static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
    private final String elasticSearchEndpointIndex;
    private final RestHighLevelClient esClient;

    /**
     * Creates a new ElasticSearchRestClient.
     *
     * @param environment Environment with properties
     */
    public ElasticSearchHighLevelRestClient(Environment environment) {
        elasticSearchEndpointAddress = environment.readEnv(ELASTICSEARCH_ENDPOINT_ADDRESS_KEY);
        elasticSearchEndpointIndex = environment.readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY);
        elasticSearchRegion = environment.readEnv(ELASTICSEARCH_ENDPOINT_REGION_KEY);

        esClient = createElasticsearchClient(SERVICE_NAME, elasticSearchRegion, elasticSearchEndpointAddress);
        logger.info(INITIAL_LOG_MESSAGE, elasticSearchEndpointAddress, elasticSearchEndpointIndex);
    }

    /**
     * Creates a new ElasticSearchRestClient.
     *
     * @param environment Environment with properties
     * @param esClient client to use for access to ElasticSearch
     */
    public ElasticSearchHighLevelRestClient(Environment environment, RestHighLevelClient esClient) {
        elasticSearchEndpointAddress = environment.readEnv(ELASTICSEARCH_ENDPOINT_ADDRESS_KEY);
        elasticSearchEndpointIndex = environment.readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY);
        elasticSearchRegion = environment.readEnv(ELASTICSEARCH_ENDPOINT_REGION_KEY);

        this.esClient = esClient;

        logger.info(INITIAL_LOG_MESSAGE, elasticSearchEndpointAddress, elasticSearchEndpointIndex);
    }


    /**
     * Searches for an term or index:term in elasticsearch index.
     * @param term search argument
     * @param results number of results
     * @throws ApiGatewayException thrown when uri is misconfigured, service i not available or interrupted
     */
    public SearchResourcesResponse searchSingleTerm(String term, String results) throws ApiGatewayException {

        try {
            QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery(term);
            final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(queryBuilder);
            final SearchRequest searchRequest = new SearchRequest(elasticSearchEndpointIndex);
            searchRequest.source(sourceBuilder);
            logger.debug(SEARCH_REQUEST_MESSAGE, elasticSearchEndpointIndex, term, searchRequest);
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchResourcesResponse searchResourcesResponse = toSearchResourcesResponse(searchResponse.toString());
            return searchResourcesResponse;

        } catch (Exception e) {
            throw new SearchException(e.getMessage(), e);
        }
    }

    /**
     * Adds or insert a document to an elasticsearch index.
     * @param document the document to be inserted
     * @throws SearchException when something goes wrong
     * */
    public void addDocumentToIndex(IndexDocument document) throws SearchException {

        logger.debug(UPSERTING_LOG_MESSAGE, document);

        try {

            String jsonDocument = document.toJsonString();

            IndexRequest indexRequest = new IndexRequest(elasticSearchEndpointIndex)
                    .source(jsonDocument, XContentType.JSON);

            UpdateRequest updateRequest = new UpdateRequest(elasticSearchEndpointIndex,  document.getIdentifier());
            logger.debug(ADD_DOCUMENT_LOG_MESSAGE, elasticSearchEndpointIndex, document.getIdentifier());

            updateRequest.upsert(indexRequest);
            updateRequest.doc(indexRequest);

            UpdateResponse updateResponse = esClient.update(updateRequest, RequestOptions.DEFAULT);
            logger.debug("updateResponse={}",updateResponse.toString());
        } catch (Exception e) {
            throw new SearchException(e.getMessage(), e);
        }
    }

    /**
     * Removes an document from Elasticsearch index.
     * @param identifier og document
     * @throws SearchException when
     */
    public void removeDocumentFromIndex(String identifier) throws SearchException {
        logger.trace(DELETE_LOG_MESSAGE, identifier);

        try {

            DeleteRequest deleteRequest = new DeleteRequest(elasticSearchEndpointIndex, identifier);
            DeleteResponse deleteResponse = esClient.delete(
                    deleteRequest, RequestOptions.DEFAULT);
            if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                logger.warn(DOCUMENT_WITH_ID_WAS_NOT_FOUND_IN_ELASTICSEARCH, identifier);
            }

        } catch (Exception e) {
            throw new SearchException(e.getMessage(), e);
        }
    }


    private SearchResourcesResponse toSearchResourcesResponse(String body) throws JsonProcessingException {
        JsonNode values = mapper.readTree(body);
        List<JsonNode> sourceList = extractSourceList(values);
        return SearchResourcesResponse.of(sourceList);
    }


    private List<JsonNode> extractSourceList(JsonNode record) {
        return toStream(record.at(HITS_JSON_POINTER))
                .map(this::extractSourceStripped)
                .collect(Collectors.toList());
    }

    @JacocoGenerated
    private JsonNode extractSourceStripped(JsonNode record) {
        JsonNode jsonNode = record.at(SOURCE_JSON_POINTER);
        return jsonNode;
    }


    private Stream<JsonNode> toStream(JsonNode node) {
        return StreamSupport.stream(node.spliterator(), false);
    }


    // Adds the interceptor to the ES REST client
    protected final RestHighLevelClient createElasticsearchClient(String serviceName,
                                                         String region,
                                                         String elasticSearchEndpoint) {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(serviceName);
        signer.setRegionName(region);
        HttpRequestInterceptor interceptor =
                new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);

        return new RestHighLevelClient(RestClient.builder(HttpHost.create(elasticSearchEndpoint))
                .setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)));
    }

}
