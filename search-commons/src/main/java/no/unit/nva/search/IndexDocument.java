package no.unit.nva.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.model.Publication;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.JsonSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Objects;

import static com.amazonaws.util.StringUtils.hasValue;
import static java.util.Objects.isNull;
import static no.unit.nva.search.constants.ApplicationConstants.SEARCH_RESOURCES_API_BASE_ADDRESS;
import static nva.commons.core.JsonUtils.objectMapper;
import static nva.commons.core.attempt.Try.attempt;

public class IndexDocument implements JsonSerializable {

    public static final String NO_TYPE_WARNING = "Resource has no publication type: ";
    public static final String INSERT_JSONNODE_ERROR_MESSAGE = "JsonNode is not an object";
    public static final String ID = "id";
    public static final String INSTANCE_TYPE_JSON_PTR = "/entityDescription/reference/publicationInstance/type";
    public static final String CONTEXT_TYPE_JSON_PTR = "/entityDescription/reference/publicationContext/type";
    public static final String IDENTIFIER_JSON_PTR = "/identifier";
    public static final String ID_JSON_PTR = "/id";
    public static final String MAIN_TITLE_JSON_PTR = "/entityDescription/mainTitle";
    public static final String PUBLISHER_ID_JSON_PTR = "/entityDescription/reference/publicationContext/publisher/id";
    public static final String JOURNAL_ID_JSON_PTR = "/entityDescription/reference/publicationContext/id";
    public static final String SERIES_ID_JSON_PTR = "/entityDescription/reference/publicationContext/series/id";
    public static final String SERIES_NAME_JSON_PTR = "/entityDescription/reference/publicationContext/series/name";

    private static final Logger logger = LoggerFactory.getLogger(IndexDocument.class);
    public static final String PATH_DELIMITER = "/";
    private final JsonNode root;

    public IndexDocument(JsonNode root) {
        this.root = root;
        assignId();
    }

    public static IndexDocument fromPublication(Publication publication) {
        return new IndexDocument(objectMapper.convertValue(publication, JsonNode.class));
    }

    public boolean hasPublicationType() {
        if (isNull(getPublicationInstanceType())) {
            logger.warn(NO_TYPE_WARNING + getIdentifier());
            return false;
        }
        return true;
    }

    public String getPublicationContextType() {
        return root.at(CONTEXT_TYPE_JSON_PTR).textValue();
    }

    public final SortableIdentifier getIdentifier() {
        return new SortableIdentifier(root.at(IDENTIFIER_JSON_PTR).textValue());
    }

    public final URI getId() {
        return URI.create(root.at(ID_JSON_PTR).textValue());
    }

    @JacocoGenerated
    public String getTitle() {
        return root.at(MAIN_TITLE_JSON_PTR).textValue();
    }

    public List<URI> getPublicationContextUris() {
        List<URI> uris = new java.util.ArrayList<>();
        if (isJournal() && isPublicationChannelId(getJournalIdStr())) {
            uris.add(getJournalURI());
        }
        if (hasPublisher()) {
            uris.add(getPublisherUri());
        }
        if (hasPublicationChannelBookSeriesId()) {
            uris.add(getBookSeriesUri());
        }
        return uris;
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(root);
    }

    @JacocoGenerated
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexDocument)) {
            return false;
        }
        IndexDocument that = (IndexDocument) o;
        return Objects.equals(root, that.root);
    }

    @JacocoGenerated
    @Override
    public String toString() {
        return toJsonString();
    }

    /**
     * JsonString.
     *
     * @return JsonString
     */
    @Override
    public String toJsonString() {
        return attempt(() -> objectMapper.writeValueAsString(addContext(root))).orElseThrow();
    }

    private JsonNode addContext(JsonNode root) {
        ObjectNode context = objectMapper.createObjectNode();
        context.put("@vocab", "https://bibsysdev.github.io/src/nva/ontology.ttl#");
        context.put("id", "@id");
        context.put("type", "@type");
        ObjectNode series = objectMapper.createObjectNode();
        series.put("@type", "@id");
        context.set("series", series);
        return ((ObjectNode) root).set("@context", context);
    }

    private boolean isPublicationChannelId(String uriCandidate) {
        return hasValue(uriCandidate) && uriCandidate.contains("publication-channels");
    }

    private boolean isJournal() {
        return "Journal".equals(getPublicationContextType());
    }

    private boolean hasPublisher() {
        return isPublicationChannelId(getPublisherId());
    }

    private String getPublisherId() {
        return root.at(PUBLISHER_ID_JSON_PTR).textValue();
    }

    private URI getPublisherUri() {
        return URI.create(getPublisherId());
    }

    private URI getJournalURI() {
        return URI.create(getJournalIdStr());
    }

    private String getJournalIdStr() {
        return root.at(JOURNAL_ID_JSON_PTR).textValue();
    }

    private String getPublicationInstanceType() {
        return root.at(INSTANCE_TYPE_JSON_PTR).textValue();
    }

    private URI getBookSeriesUri() {
        return URI.create(getBookSeriesUriStr());
    }

    private String getBookSeriesUriStr() {
        return root.at(SERIES_ID_JSON_PTR).textValue();
    }

    private boolean hasPublicationChannelBookSeriesId() {
        return isPublicationChannelId(getBookSeriesUriStr());
    }

    private void assignId() {
        URI id = URI.create(SEARCH_RESOURCES_API_BASE_ADDRESS + PATH_DELIMITER + getIdentifier());
        if (root.isObject()) {
            ((ObjectNode) root).put(ID, id.toString());
        } else {
            throw new IllegalArgumentException(INSERT_JSONNODE_ERROR_MESSAGE);
        }
    }

}
