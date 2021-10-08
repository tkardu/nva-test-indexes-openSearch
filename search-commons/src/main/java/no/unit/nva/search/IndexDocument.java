package no.unit.nva.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.model.Publication;
import no.unit.nva.utils.IndexDocumentWrapperLinkedData;
import no.unit.nva.utils.UriRetriever;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.JsonSerializable;
import nva.commons.core.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Objects;

import static com.amazonaws.util.StringUtils.hasValue;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static no.unit.nva.search.constants.ApplicationConstants.PUBLICATION_API_BASE_ADDRESS;
import static nva.commons.core.JsonUtils.objectMapper;
import static nva.commons.core.attempt.Try.attempt;

@SuppressWarnings("PMD.GodClass")
public final class IndexDocument implements JsonSerializable {

    public static final String NO_TYPE_WARNING = "Resource has no publication type: ";
    public static final String ID_FIELD_NAME = "id";
    public static final String INSTANCE_TYPE_JSON_PTR = "/entityDescription/reference/publicationInstance/type";
    public static final String CONTEXT_TYPE_JSON_PTR = "/entityDescription/reference/publicationContext/type";
    public static final String IDENTIFIER_JSON_PTR = "/identifier";
    public static final String ID_JSON_PTR = "/id";
    public static final String MAIN_TITLE_JSON_PTR = "/entityDescription/mainTitle";
    public static final String PUBLISHER_ID_JSON_PTR = "/entityDescription/reference/publicationContext/publisher/id";
    public static final String JOURNAL_ID_JSON_PTR = "/entityDescription/reference/publicationContext/id";
    public static final String SERIES_ID_JSON_PTR = "/entityDescription/reference/publicationContext/series/id";
    public static final String NO_TITLE_WARNING = "Resource has no title: ";
    public static final String PATH_DELIMITER = "/";
    private static final Logger logger = LoggerFactory.getLogger(IndexDocument.class);
    private static final UriRetriever uriRetriever = new UriRetriever();

    private final JsonNode indexDocumentRootNode;

    public IndexDocument(JsonNode root) {
        this.indexDocumentRootNode = root;
        assignIdToRootNode(indexDocumentRootNode);
    }

    public static IndexDocument fromPublication(Publication publication) {
        return fromPublication(uriRetriever, publication);
    }

    public static IndexDocument fromPublication(UriRetriever uriRetriever, Publication publication) {
        final JsonNode jsonNode = getJsonNode(publication);

        assignIdToRootNode(jsonNode);
        String enrichedJson =
                attempt(() -> new IndexDocumentWrapperLinkedData(uriRetriever).toFramedJsonLd(jsonNode))
                        .orElseThrow();

        return new IndexDocument(attempt(() -> objectMapper.readTree(enrichedJson)).orElseThrow());
    }

    public static SortableIdentifier getIdentifier(JsonNode root) {
        return new SortableIdentifier(root.at(IDENTIFIER_JSON_PTR).textValue());
    }

    private static JsonNode getJsonNode(Publication publication) {
        return objectMapper.convertValue(publication, JsonNode.class);
    }

    public static boolean hasPublicationType(JsonNode root) {
        if (isNull(getPublicationInstanceType(root))) {
            logger.warn(NO_TYPE_WARNING + getIdentifier(root));
            return false;
        }
        return true;
    }

    public static String getPublicationContextType(JsonNode root) {
        return root.at(CONTEXT_TYPE_JSON_PTR).textValue();
    }

    private static String getTitle(JsonNode root) {
        return root.at(MAIN_TITLE_JSON_PTR).textValue();
    }

    public static List<URI> getPublicationContextUris(JsonNode indexDocument) {
        List<URI> uris = new java.util.ArrayList<>();
        if (isJournal(indexDocument) && isPublicationChannelId(getJournalIdStr(indexDocument))) {
            uris.add(getJournalURI(indexDocument));
        }
        if (hasPublisher(indexDocument)) {
            uris.add(getPublisherUri(indexDocument));
        }
        if (hasPublicationChannelBookSeriesId(indexDocument)) {
            uris.add(getBookSeriesUri(indexDocument));
        }
        return uris;
    }

    public static void assignIdToRootNode(JsonNode root) {
        if (hasNoId(root) && hasIdentifier(root)) {
            URI id = URI.create(
                    mergeStringsWithDelimiter(
                            PUBLICATION_API_BASE_ADDRESS, requireNonNull(getIdentifier(root)).toString()));
            ((ObjectNode) root).put(ID_FIELD_NAME, id.toString());
        }
    }

    @JacocoGenerated
    public static String mergeStringsWithDelimiter(String publicationApiBaseAddress, String identifier) {
        return publicationApiBaseAddress.endsWith(PATH_DELIMITER)
                ? publicationApiBaseAddress + identifier
                : publicationApiBaseAddress + PATH_DELIMITER + identifier;
    }

    public static String toJsonString(JsonNode root) {
        return attempt(() -> objectMapper.writeValueAsString(addContext(root))).orElseThrow();
    }

    private static JsonNode addContext(JsonNode root) {
        if (!isNull(root)) {
            ObjectNode context = objectMapper.createObjectNode();
            context.put("@vocab", "https://bibsysdev.github.io/src/nva/ontology.ttl#");
            context.put("id", "@id");
            context.put("type", "@type");
            ObjectNode series = objectMapper.createObjectNode();
            series.put("@type", "@id");
            context.set("series", series);
            ((ObjectNode) root).set("@context", context);
        }
        return root;
    }

    private static boolean isPublicationChannelId(String uriCandidate) {
        return hasValue(uriCandidate) && uriCandidate.contains("publication-channels");
    }

    private static boolean isJournal(JsonNode root) {
        return "Journal".equals(getPublicationContextType(root));
    }

    private static boolean hasPublisher(JsonNode root) {
        return isPublicationChannelId(getPublisherId(root));
    }

    private static String getPublisherId(JsonNode root) {
        return root.at(PUBLISHER_ID_JSON_PTR).textValue();
    }

    private static URI getPublisherUri(JsonNode root) {
        return URI.create(getPublisherId(root));
    }

    private static URI getJournalURI(JsonNode root) {
        return URI.create(getJournalIdStr(root));
    }

    private static String getJournalIdStr(JsonNode root) {
        return root.at(JOURNAL_ID_JSON_PTR).textValue();
    }

    private static String getPublicationInstanceType(JsonNode root) {
        return root.at(INSTANCE_TYPE_JSON_PTR).textValue();
    }

    private static URI getBookSeriesUri(JsonNode root) {
        return URI.create(getBookSeriesUriStr(root));
    }

    private static String getBookSeriesUriStr(JsonNode root) {
        return root.at(SERIES_ID_JSON_PTR).textValue();
    }

    private static boolean hasPublicationChannelBookSeriesId(JsonNode root) {
        return isPublicationChannelId(getBookSeriesUriStr(root));
    }

    private static boolean hasNoId(JsonNode root) {
        return !isNull(root) && !root.has(ID_FIELD_NAME);
    }

    private static boolean hasIdentifier(JsonNode root) {
        return !isNull(root) && root.has("identifier");
    }

    public URI getId() {
        return URI.create(indexDocumentRootNode.at(ID_JSON_PTR).textValue());
    }

    public SortableIdentifier getIdentifier() {
        return getIdentifier(indexDocumentRootNode);
    }

    public boolean hasPublicationType() {
        return hasPublicationType(indexDocumentRootNode);
    }

    public String getPublicationContextType() {
        return getPublicationContextType(indexDocumentRootNode);
    }

    @JacocoGenerated
    public boolean hasTitle() {
        if (StringUtils.isBlank(getTitle(indexDocumentRootNode))) {
            logger.warn(NO_TITLE_WARNING + getIdentifier(indexDocumentRootNode));
            return false;
        }
        return true;
    }

    public List<URI> getPublicationContextUris() {
        return getPublicationContextUris(indexDocumentRootNode);
    }

    /**
     * JsonString.
     *
     * @return JsonString
     */
    @Override
    public String toJsonString() {
        return toJsonString(indexDocumentRootNode);
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(indexDocumentRootNode);
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
        return Objects.equals(indexDocumentRootNode, that.indexDocumentRootNode);
    }

    @Override
    public String toString() {
        return toJsonString();
    }

    public JsonNode asJsonNode() {
        return indexDocumentRootNode;
    }

}
