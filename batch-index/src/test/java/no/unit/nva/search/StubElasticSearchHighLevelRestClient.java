package no.unit.nva.search;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import no.unit.nva.search.exception.SearchException;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;

public class StubElasticSearchHighLevelRestClient extends ElasticSearchHighLevelRestClient {

    public static final int IGNORED_PROCESSING_TIME = 123;
    Map<String, IndexDocument> index = new ConcurrentHashMap<>();

    public StubElasticSearchHighLevelRestClient() {
        super();
    }

    @Override
    public void addDocumentToIndex(IndexDocument document) {
        index.put(document.getId().toString(), document);
    }

    @Override
    public void removeDocumentFromIndex(String identifier) {
        index.remove(identifier);
    }

    @Override
    public List<BulkResponse> batchInsert(List<IndexDocument> indexDocuments) {
        indexDocuments.forEach(doc -> index.put(doc.getId().toString(), doc));
        return constructSampleBulkResponse(indexDocuments);
    }

    public Map<String, IndexDocument> getIndex() {
        return index;
    }

    private List<BulkResponse> constructSampleBulkResponse(List<IndexDocument> indexDocuments) {
        DocWriteResponse doc = null;
        List<BulkItemResponse> responses = indexDocuments.stream()
            .map(id -> new BulkItemResponse(id.hashCode(), OpType.UPDATE, doc))
            .collect(Collectors.toList());

        BulkItemResponse[] responsesArray = responses.toArray(BulkItemResponse[]::new);
        return List.of(new BulkResponse(responsesArray, IGNORED_PROCESSING_TIME));
    }
}
