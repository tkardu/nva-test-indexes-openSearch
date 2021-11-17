package no.unit.nva.indexing.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import no.unit.nva.search.IndexingClient;
import no.unit.nva.search.models.IndexDocument;
import org.elasticsearch.action.bulk.BulkResponse;

/**
 * Faking the Indexing Client instead of the ElasticSearch client because faking the ElasticSearch client is difficult.
 */
public class FakeIndexingClient extends IndexingClient {

    private final Map<String, Set<JsonNode>> indexContents;

    public FakeIndexingClient() {
        indexContents = new ConcurrentHashMap<>();
    }

    @Override
    public Void addDocumentToIndex(IndexDocument indexDocument) throws IOException {
        if (!indexContents.containsKey(indexDocument.getIndexName())) {
            indexContents.put(indexDocument.getIndexName(), new HashSet<>());
        }

        indexContents.get(indexDocument.getIndexName()).add(indexDocument.getResource());
        return null;
    }

    @Override
    public void removeDocumentFromIndex(String identifier) {
        // TODO remove document from index needs an index name to delete the document from.
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<BulkResponse> batchInsert(Stream<IndexDocument> contents) {
        // Batch insertion has its own fake client that will be merged in subsequent PR.
        throw new UnsupportedOperationException();
    }

    public Set<JsonNode> listAllDocuments(String indexName) {
        return this.indexContents.get(indexName);
    }
}
