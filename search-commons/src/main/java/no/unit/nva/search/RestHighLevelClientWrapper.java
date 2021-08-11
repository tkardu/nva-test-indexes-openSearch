package no.unit.nva.search;

import java.io.IOException;
import nva.commons.core.JacocoGenerated;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for avoiding mocking/spying the ES final classes.
 */
public class RestHighLevelClientWrapper {

    private static final Logger logger = LoggerFactory.getLogger(RestHighLevelClientWrapper.class);
    private final RestHighLevelClient client;

    public RestHighLevelClientWrapper(RestHighLevelClient client) {
        this.client = client;
    }

    public RestHighLevelClientWrapper(RestClientBuilder clientBuilder) {
        this.client = new RestHighLevelClient(clientBuilder);
    }

    /**
     * Use this method only to experiment and to extend the functionality of the wrapper.
     *
     * @return the contained client
     */
    @JacocoGenerated
    public RestHighLevelClient getClient() {
        logger.warn("Use getClient only for finding which methods you need to add to the wrapper");
        return this.client;
    }

    @JacocoGenerated
    public SearchResponse search(SearchRequest searchRequest, RequestOptions requestOptions) throws IOException {
        return client.search(searchRequest, requestOptions);
    }

    @JacocoGenerated
    public IndexResponse index(IndexRequest updateRequest, RequestOptions requestOptions) throws IOException {
        return client.index(updateRequest, requestOptions);
    }

    @JacocoGenerated
    public DeleteResponse delete(DeleteRequest deleteRequest, RequestOptions requestOptions) throws IOException {
        return client.delete(deleteRequest, requestOptions);
    }

    @JacocoGenerated
    public UpdateResponse update(UpdateRequest updateRequest, RequestOptions requestOptions) throws IOException {
        return client.update(updateRequest, requestOptions);
    }

    @JacocoGenerated
    public IndicesClientWrapper indices() {
        return new IndicesClientWrapper(client.indices());
    }
}
