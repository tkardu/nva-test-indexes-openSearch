package no.unit.nva.indexing.handlers;

import com.fasterxml.jackson.annotation.JsonProperty;
import no.unit.nva.identifiers.SortableIdentifier;

public class IndexDocumentMetadata {

    public static final String INDEX_FIELD = "index";
    public static final String DOCUMENT_IDENTIFIER = "documentIdentifier";

    @JsonProperty(INDEX_FIELD)
    private final String index;
    @JsonProperty(DOCUMENT_IDENTIFIER)
    private final SortableIdentifier documentIdentifier;

    public IndexDocumentMetadata(@JsonProperty(INDEX_FIELD) String index,
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
}
