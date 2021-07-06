package no.unit.nva.search;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import no.unit.nva.search.exception.SearchException;

public class StubElasticSearchHighLevelRestClient extends ElasticSearchHighLevelRestClient {

    Map<String, IndexDocument> index = new ConcurrentHashMap<>();

    public StubElasticSearchHighLevelRestClient() {
        super();
    }

    @Override
    public void addDocumentToIndex(IndexDocument document) throws SearchException {
        index.put(document.getId().toString(), document);
    }

    @Override
    public void removeDocumentFromIndex(String identifier) {
        index.remove(identifier);
    }

    public Map<String, IndexDocument> getIndex() {
        return index;
    }
}
