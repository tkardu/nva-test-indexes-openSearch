package no.unit.nva.search;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import no.unit.nva.search.models.NewIndexDocument;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;

public class StubElasticSearchHighLevelRestClient extends ElasticSearchHighLevelRestClient {

    public static final int IGNORED_PROCESSING_TIME = 123;
    Map<String, JsonNode> index = new ConcurrentHashMap<>();

    public StubElasticSearchHighLevelRestClient() {
        super();
    }

    @Override
    public void addDocumentToIndex(NewIndexDocument document) {
        index.put(document.getDocumentIdentifier(), document.getResource());
    }

    @Override
    public void removeDocumentFromIndex(String identifier) {
        index.remove(identifier);
    }

    @Override
    public Stream<BulkResponse> batchInsert(Stream<NewIndexDocument> indexDocuments) {
        Stream<NewIndexDocument> indexedDocuments = indexDocuments
            .peek(this::addDocumentToIndex);
        return constructSampleBulkResponse(indexedDocuments).stream();
    }

    public Map<String, JsonNode> getIndex() {
        return index;
    }

    private List<BulkResponse> constructSampleBulkResponse(Stream<NewIndexDocument> indexDocuments) {
        DocWriteResponse response = null;
        List<BulkItemResponse> responses = indexDocuments
            .map(doc -> new BulkItemResponse(doc.hashCode(), OpType.UPDATE, response))
            .collect(Collectors.toList());
        BulkItemResponse[] responsesArray = responses.toArray(BulkItemResponse[]::new);
        return List.of(new BulkResponse(responsesArray, IGNORED_PROCESSING_TIME));
    }
}
