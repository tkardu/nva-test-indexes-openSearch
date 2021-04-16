package no.unit.nva.search;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import no.unit.nva.search.exception.SearchException;
import nva.commons.core.Environment;

public class StubElasticSearchHighLevelRestClient extends ElasticSearchHighLevelRestClient {

    Map<String, IndexDocument> index = new ConcurrentHashMap<>();

    public StubElasticSearchHighLevelRestClient(Environment environment) {
        super(environment);
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
