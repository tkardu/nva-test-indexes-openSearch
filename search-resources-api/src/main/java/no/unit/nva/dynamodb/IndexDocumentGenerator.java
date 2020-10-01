package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.search.IndexContributor;
import no.unit.nva.search.IndexDate;
import no.unit.nva.search.IndexDocument;
import nva.commons.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public static final String MISSING_FIELD_LOGGER_WARNING_TEMPLATE =
            "The data from DynamoDB was incomplete, missing required field {} on id: {}, ignoring entry";
    public static final String TYPE = "type";
    public static final String TITLE = "title";
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
                .withDate(new IndexDate(record))
                .withTitle(extractTitle(record, id));
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

    private static void logMissingField(UUID id, String field) {
        logger.warn(MISSING_FIELD_LOGGER_WARNING_TEMPLATE, field, id);
    }

    private static IndexContributor generateIndexContributor(String identifier, String name) {
        return new IndexContributor.Builder()
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
