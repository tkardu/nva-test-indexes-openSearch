package no.unit.nva.search.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import no.unit.nva.identifiers.SortableIdentifier;
import nva.commons.core.JacocoGenerated;

public class EventConsumptionAttributes {

    public static final String INDEX_FIELD = "index";
    public static final String DOCUMENT_IDENTIFIER = "documentIdentifier";

    @JsonProperty(INDEX_FIELD)
    private final String index;
    @JsonProperty(DOCUMENT_IDENTIFIER)
    private final SortableIdentifier documentIdentifier;

    public EventConsumptionAttributes(@JsonProperty(INDEX_FIELD) String index,
                                      @JsonProperty(DOCUMENT_IDENTIFIER) SortableIdentifier documentIdentifier) {
        this.index = index;
        this.documentIdentifier = documentIdentifier;
    }

    public String getIndex() {
        return index;
    }

    public SortableIdentifier getDocumentIdentifier() {
        return documentIdentifier;
    }

    @JacocoGenerated
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EventConsumptionAttributes)) {
            return false;
        }
        EventConsumptionAttributes that = (EventConsumptionAttributes) o;
        return Objects.equals(getIndex(), that.getIndex()) && Objects.equals(getDocumentIdentifier(),
                                                                             that.getDocumentIdentifier());
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(getIndex(), getDocumentIdentifier());
    }
}
