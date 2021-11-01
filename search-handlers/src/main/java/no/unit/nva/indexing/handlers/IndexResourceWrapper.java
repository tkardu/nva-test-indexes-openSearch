package no.unit.nva.indexing.handlers;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Objects;

import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;

public class IndexResourceWrapper {

    private static final JsonPointer TYPE_JSON_POINTER = JsonPointer.compile("/type");
    private static final JsonPointer IDENTIFIER_JSON_POINTER = JsonPointer.compile("/identifier");
    public static final String ERROR_PARSING_JSON = "Error parsing json";
    public static final String MISSING_IDENTIFIER_IN_RESOURCE = "Missing identifier in resource";
    public static final String MISSING_INDEX_NAME_IN_RESOURCE = "Missing index name in resource";

    private final JsonNode resource;

    public IndexResourceWrapper(String resource) {
        this(fromJsonString(resource));
    }

    public IndexResourceWrapper(JsonNode resource) {
        this.resource = resource;
    }

    public String getIndexName() {
        JsonNode node = resource.at(TYPE_JSON_POINTER);
        if (node.isValueNode() && Objects.nonNull(node.textValue())) {
            return node.textValue();
        }
        throw new RuntimeException(MISSING_INDEX_NAME_IN_RESOURCE);
    }

    public String getIdentifier() {
        JsonNode node = resource.at(IDENTIFIER_JSON_POINTER);
        if (node.isValueNode() && Objects.nonNull(node.textValue())) {
            return node.textValue();
        }
        throw new RuntimeException(MISSING_IDENTIFIER_IN_RESOURCE);
    }

    public String toJsonString() {
        try {
            return objectMapperWithEmpty.writeValueAsString(resource);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static JsonNode fromJsonString(String json) {
        try {
            return objectMapperWithEmpty.readTree(json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(ERROR_PARSING_JSON, e);
        }
    }

    public IndexRequest toIndexRequest() {
        return new IndexRequest(getIndexName())
                .source(toJsonString(), XContentType.JSON)
                .id(getIdentifier());
    }
}
