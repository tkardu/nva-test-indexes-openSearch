package no.unit.nva.indexing.testutils;

import static nva.commons.core.attempt.Try.attempt;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import no.unit.nva.search.IndexingClient;
import no.unit.nva.search.models.IndexDocument;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;

/**
 * Faking the Indexing Client instead of the ElasticSearch client because faking the ElasticSearch client is difficult.
 */
public class FakeIndexingClient extends IndexingClient {

    private static final long IGNORED_PROCESSING_TIME = 0;
    private final Map<String, Set<JsonNode>> indexContents;

    public FakeIndexingClient() {
        super(null);
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
    public Stream<BulkResponse> batchInsert(Stream<IndexDocument> indexDocuments) {
        var collectedDocuments = indexDocuments.collect(Collectors.toList());
        for (IndexDocument collectedDocument : collectedDocuments) {
            attempt(() -> addDocumentToIndex(collectedDocument)).orElseThrow();
        }

        return constructSampleBulkResponse(collectedDocuments).stream();
    }

    public Set<JsonNode> getIndex(String indexName) {
        return this.indexContents.getOrDefault(indexName, Collections.emptySet());
    }

    private List<BulkResponse> constructSampleBulkResponse(Collection<IndexDocument> indexDocuments) {
        DocWriteResponse response = null;
        List<BulkItemResponse> responses = indexDocuments
            .stream()
            .map(doc -> new BulkItemResponse(doc.hashCode(), OpType.UPDATE, response))
            .collect(Collectors.toList());
        BulkItemResponse[] responsesArray = responses.toArray(BulkItemResponse[]::new);
        return List.of(new BulkResponse(responsesArray, IGNORED_PROCESSING_TIME));
    }

    public Set<JsonNode> listAllDocuments(String indexName) {
        return this.indexContents.get(indexName);
    }
}
