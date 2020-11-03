package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.search.IndexContributor;
import no.unit.nva.search.IndexDate;
import no.unit.nva.search.IndexDocument;
import no.unit.nva.search.IndexPublisher;
import nva.commons.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public final class IndexDocumentGenerator extends IndexDocument {

    public static final String CONTRIBUTOR_LIST_JSON_POINTER = "/entityDescription/m/contributors/l";
    public static final String CONTRIBUTOR_ARP_ID_JSON_POINTER = "/m/identity/m/arpId/s";
    public static final String CONTRIBUTOR_NAME_JSON_POINTER = "/m/identity/m/name/s";
    public static final String IDENTIFIER_JSON_POINTER = "/identifier/s";
    public static final String MAIN_TITLE_JSON_POINTER = "/entityDescription/m/mainTitle/s";
    public static final String TYPE_JSON_POINTER = "/entityDescription/m/reference/m/publicationInstance/m/type/s";
    public static final String DOI_JSON_POINTER = "/entityDescription/m/reference/m/doi/s";

    public static final String OWNER_JSON_POINTER = "/owner/s";
    public static final String DESCRIPTION_JSON_POINTER = "/entityDescription/m/description/s";
    public static final String PUBLICATION_ABSTRACT_JSON_POINTER = "/entityDescription/m/abstract/s";
    public static final String PUBLISHER_ID_JSON_POINTER = "/publisher/m/id/s";
    public static final String PUBLISHER_TYPE_JSON_POINTER = "/publisher/m/type/s";

    public static final String MISSING_FIELD_LOGGER_WARNING_TEMPLATE =
            "The data from DynamoDB was incomplete, missing required field {} on id: {}, ignoring entry";
    public static final String TYPE = "type";
    public static final String TITLE = "title";
    public static final String OWNER = "owner";
    public static final String DESCRIPTION = "description";
    public static final String ABSTRACT = "abstract";

    private static final ObjectMapper mapper = JsonUtils.objectMapper;
    private static final Logger logger = LoggerFactory.getLogger(IndexDocumentGenerator.class);

    private IndexDocumentGenerator(IndexDocument.Builder builder) {
        super(builder);
    }

    /**
     * Transforms a DynamoDB streamrecord into IndexDocument.
     *
     * @param streamRecord of the original dynamoDB record
     * @return A document usable for indexing in elasticsearch
     */
    protected static IndexDocumentGenerator fromStreamRecord(DynamodbEvent.DynamodbStreamRecord streamRecord) {
        JsonNode record = toJsonNode(streamRecord);

        UUID id = extractId(record);

        Builder builder = new Builder()
                .withId(id)
                .withType(extractType(record, id))
                .withContributors(extractContributors(record))
                .withPublishedDate(new IndexDate(record))
                .withTitle(extractTitle(record, id))
                .withOwner(extractOwner(record, id))
                .withDescription(extractDescription(record, id))
                .withAbstract(extractAbstract(record, id))
                .withPublisher(extractPublisher(record));

        Optional<URI> optionalURI = extractDoi(record);
        if (optionalURI.isPresent()) {
            builder = builder.withDoi(optionalURI.get());
        }

        return new IndexDocumentGenerator(builder);
    }

    private static List<IndexContributor> extractContributors(JsonNode record) {
        return toStream(record.at(CONTRIBUTOR_LIST_JSON_POINTER))
                .map(IndexDocumentGenerator::extractIndexContributor)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static IndexContributor extractIndexContributor(JsonNode jsonNode) {
        String identifier = textFromNode(jsonNode, CONTRIBUTOR_ARP_ID_JSON_POINTER);
        String name = textFromNode(jsonNode, CONTRIBUTOR_NAME_JSON_POINTER);
        return nonNull(name) ? generateIndexContributor(identifier, name) : null;
    }

    private static UUID extractId(JsonNode record) {
        return Optional.ofNullable(record)
                .map(rec -> textFromNode(rec, IDENTIFIER_JSON_POINTER))
                .map(UUID::fromString)
                .orElseThrow();
    }

    private static String extractTitle(JsonNode record, UUID id) {
        var title = textFromNode(record, MAIN_TITLE_JSON_POINTER);
        if (isNull(title)) {
            logMissingField(id, TITLE);
        }
        return title;
    }

    private static String extractType(JsonNode record, UUID id) {
        var type = textFromNode(record, TYPE_JSON_POINTER);
        if (isNull(type)) {
            logMissingField(id, TYPE);
        }
        return type;
    }

    private static Optional<URI> extractDoi(JsonNode record) {
        return Optional.ofNullable(record)
                .map(rec -> textFromNode(rec, DOI_JSON_POINTER))
                .map(URI::create);
    }

    private static URI extractPublisherId(JsonNode record) {
        return Optional.ofNullable(record)
                .map(rec -> textFromNode(rec, PUBLISHER_ID_JSON_POINTER))
                .map(URI::create)
                .orElseThrow();
    }


    private static String extractOwner(JsonNode record, UUID id) {
        var owner = textFromNode(record, OWNER_JSON_POINTER);
        if (isNull(owner)) {
            logMissingField(id, OWNER);
        }
        return owner;
    }

    private static String extractDescription(JsonNode record, UUID id) {
        var description = textFromNode(record, DESCRIPTION_JSON_POINTER);
        if (isNull(description)) {
            logMissingField(id, DESCRIPTION);
        }
        return description;
    }

    private static String extractAbstract(JsonNode record, UUID id) {
        var publicationAbstract = textFromNode(record, PUBLICATION_ABSTRACT_JSON_POINTER);
        if (isNull(publicationAbstract)) {
            logMissingField(id, ABSTRACT);
        }
        return publicationAbstract;
    }

    private static IndexPublisher extractPublisher(JsonNode record) {
        URI publisherId = extractPublisherId(record);
        String publisherType = textFromNode(record, PUBLISHER_TYPE_JSON_POINTER);
        return nonNull(publisherId) ? generateIndexPublisher(publisherId, publisherType) : null;
    }


    private static void logMissingField(UUID id, String field) {
        logger.warn(MISSING_FIELD_LOGGER_WARNING_TEMPLATE, field, id);
    }

    private static IndexContributor generateIndexContributor(String identifier, String name) {
        return new IndexContributor.Builder()
                .withId(identifier)
                .withName(name)
                .build();
    }

    private static IndexPublisher generateIndexPublisher(URI identifier, String type) {
        String name = type; // TODO sg - fix lookup of publisher name
        return new IndexPublisher.Builder()
                .withId(identifier)
                .withName(name)
                .build();
    }



    private static String textFromNode(JsonNode jsonNode, String jsonPointer) {
        JsonNode json = jsonNode.at(jsonPointer);
        return isPopulated(json) ? json.asText() : null;
    }

    private static boolean isPopulated(JsonNode json) {
        return !json.isNull() && !json.asText().isBlank();
    }

    private static JsonNode toJsonNode(DynamodbEvent.DynamodbStreamRecord streamRecord) {
        return mapper.valueToTree(streamRecord.getDynamodb().getNewImage());
    }

    private static Stream<JsonNode> toStream(JsonNode contributors) {
        return StreamSupport.stream(contributors.spliterator(), false);
    }

    public IndexDocument toIndexDocument() {
        return this;
    }
}
