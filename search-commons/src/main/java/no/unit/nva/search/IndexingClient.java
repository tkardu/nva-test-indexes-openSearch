package no.unit.nva.search;

import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_ENDPOINT_ADDRESS;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_ENDPOINT_INDEX;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_REGION;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTIC_SEARCH_SERVICE_NAME;
import static nva.commons.core.attempt.Try.attempt;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import java.io.IOException;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import no.unit.nva.search.models.IndexDocument;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.attempt.Try;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingClient {

    public static final String INITIAL_LOG_MESSAGE = "using Elasticsearch endpoint {} and index {}";
    public static final String DOCUMENT_WITH_ID_WAS_NOT_FOUND_IN_ELASTICSEARCH
        = "Document with id={} was not found in elasticsearch";
    public static final int BULK_SIZE = 100;
    public static final boolean SEQUENTIAL = false;
    private static final Logger logger = LoggerFactory.getLogger(IndexingClient.class);
    private static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
    private final RestHighLevelClientWrapper elasticSearchClient;

    @JacocoGenerated
    public IndexingClient() {
        elasticSearchClient = createElasticsearchClientWithInterceptor();
        logger.info(INITIAL_LOG_MESSAGE, ELASTICSEARCH_ENDPOINT_ADDRESS, ELASTICSEARCH_ENDPOINT_INDEX);
    }

    /**
     * Creates a new ElasticSearchRestClient.
     *
     * @param elasticSearchClient client to use for access to ElasticSearch
     */
    public IndexingClient(RestHighLevelClientWrapper elasticSearchClient) {

        this.elasticSearchClient = elasticSearchClient;
        logger.info(INITIAL_LOG_MESSAGE, ELASTICSEARCH_ENDPOINT_ADDRESS, ELASTICSEARCH_ENDPOINT_INDEX);
    }

    public Void addDocumentToIndex(IndexDocument indexDocument) throws IOException {
        elasticSearchClient.index(indexDocument.toIndexRequest(), RequestOptions.DEFAULT);
        return null;
    }

    /**
     * Removes a document from Elasticsearch index.
     *
     * @param identifier og document
     */
    public void removeDocumentFromIndex(String identifier) throws IOException {
        DeleteResponse deleteResponse = elasticSearchClient
            .delete(new DeleteRequest(ELASTICSEARCH_ENDPOINT_INDEX, identifier),
                    RequestOptions.DEFAULT);
        if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            logger.warn(DOCUMENT_WITH_ID_WAS_NOT_FOUND_IN_ELASTICSEARCH, identifier);
        }
    }

    /**
     * Create Index in Elastic search based on the name.
     *
     * @param indexName name of the index needs to create.
     */
    public Void createIndex(String indexName) throws IOException {
        var indicesClientWrapper = getIndicesClientWrapper();
        indicesClientWrapper.create(new CreateIndexRequest(indexName), RequestOptions.DEFAULT);
        return null;
    }

    public Stream<BulkResponse> batchInsert(Stream<IndexDocument> contents) {
        var batches = splitStreamToBatches(contents);
        return batches.map(attempt(this::insertBatch)).map(Try::orElseThrow);
    }

    /**
     * Provides an IndicesClientWrapper which can be used to access the Indices API. See Indices API on elastic.co
     *
     * @return IndicesClientWrapper.
     */
    private IndicesClientWrapper getIndicesClientWrapper() {
        return elasticSearchClient.indices();
    }

    @JacocoGenerated
    private RestHighLevelClientWrapper createElasticsearchClientWithInterceptor() {
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

    private Stream<List<IndexDocument>> splitStreamToBatches(Stream<IndexDocument> indexDocuments) {
        UnmodifiableIterator<List<IndexDocument>> bulks = Iterators.partition(
            indexDocuments.iterator(), BULK_SIZE);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(bulks, Spliterator.ORDERED), SEQUENTIAL);
    }

    private BulkResponse insertBatch(List<IndexDocument> bulk) throws IOException {
        List<IndexRequest> indexRequests = bulk.stream()
            .parallel()
            .map(IndexDocument::toIndexRequest)
            .collect(Collectors.toList());

        BulkRequest request = new BulkRequest();
        indexRequests.forEach(request::add);
        request.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        request.waitForActiveShards(ActiveShardCount.ONE);
        return elasticSearchClient.bulk(request, RequestOptions.DEFAULT);
    }

    @JacocoGenerated
    private AWS4Signer getAws4Signer() {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(ELASTIC_SEARCH_SERVICE_NAME);
        signer.setRegionName(ELASTICSEARCH_REGION);
        return signer;
    }
}
