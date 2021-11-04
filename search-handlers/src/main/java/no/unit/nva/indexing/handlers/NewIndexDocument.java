package no.unit.nva.indexing.handlers;

import static no.unit.nva.indexing.handlers.IndexingConfig.objectMapper;
import static nva.commons.core.attempt.Try.attempt;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.Serializable;
import java.util.Optional;
import no.unit.nva.identifiers.SortableIdentifier;
import nva.commons.core.StringUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class NewIndexDocument implements Serializable {

    public static final String BODY = "body";
    public static final String CONSUMPTION_ATTRIBUTES = "consumptionAttributes";
    public static final String MISSING_IDENTIFIER_IN_RESOURCE = "Missing identifier in resource";
    public static final String MISSING_INDEX_NAME_IN_RESOURCE = "Missing index name in resource";
    @JsonProperty(CONSUMPTION_ATTRIBUTES)
    private final IndexDocumentMetadata consumptionAttributes;
    @JsonProperty(BODY)
    private final JsonNode resource;

    @JsonCreator
    public NewIndexDocument(@JsonProperty(CONSUMPTION_ATTRIBUTES) IndexDocumentMetadata consumptionAttributes,
                            @JsonProperty(BODY) JsonNode resource) {
        this.consumptionAttributes = consumptionAttributes;
        this.resource = resource;
    }

    @JsonIgnore
    public String getIndexName() {
        return Optional.ofNullable(consumptionAttributes.getIndex())
            .filter(StringUtils::isNotBlank)
            .orElseThrow(() -> new RuntimeException(MISSING_INDEX_NAME_IN_RESOURCE));
    }

    @JsonIgnore
    public String getDocumentIdentifier() {
        return Optional.ofNullable(consumptionAttributes.getDocumentIdentifier())
            .map(SortableIdentifier::toString)
            .orElseThrow(() -> new RuntimeException(MISSING_IDENTIFIER_IN_RESOURCE));
    }

    public IndexRequest toIndexRequest() {
        return new IndexRequest(getIndexName())
            .source(serializeResource(), XContentType.JSON)
            .id(getDocumentIdentifier());
    }

    private String serializeResource() {
        return attempt(() -> objectMapper.writeValueAsString(resource)).orElseThrow();
    }

    public static NewIndexDocument fromJsonString(String json) {
        return attempt(() -> objectMapper.readValue(json, NewIndexDocument.class)).orElseThrow();
    }
}
