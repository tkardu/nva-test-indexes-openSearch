package no.unit.nva.search;

import no.unit.nva.search.exception.SearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Highlevel client to hide details about persisting search.
 */
public class SearchClient {

    private static final Logger logger = LoggerFactory.getLogger(SearchClient.class);
    private final ElasticSearchHighLevelRestClient elasticSearchRestClient;

    /**
     * Creates an SearchClient bsased on elasicSearch.
     * @param elasticSearchRestClient configurated client to use for accessing elasticsearch
     */
    public SearchClient(ElasticSearchHighLevelRestClient elasticSearchRestClient) {
        this.elasticSearchRestClient = elasticSearchRestClient;
    }

    /**
     * Persists the searchable document to a persistent storage.
     * @param indexDocument generated indexDocument to be persisted
     * @return the persisted document
     */
    public IndexDocument persist(IndexDocument indexDocument) {
        try {
            elasticSearchRestClient.addDocumentToIndex(indexDocument);
        } catch (SearchException e) {
            logger.error(e.getMessage(), e);
        }
        return indexDocument;
    }
}
