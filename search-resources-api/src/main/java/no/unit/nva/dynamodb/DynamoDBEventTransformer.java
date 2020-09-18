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

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class DynamoDBEventTransformer {

    private static final ObjectMapper mapper = JsonUtils.objectMapper;

    public static final String CONTRIBUTOR_LIST_JSON_POINTER = "/entityDescription/m/contributors/l";
    public static final String CONTRIBUTOR_ARP_ID_JSON_POINTER = "/m/identity/m/arpId/s";
    public static final String CONTRIBUTOR_NAME_JSON_POINTER = "/m/identity/m/name/s";
    public static final String IDENTIFIER_JSON_POINTER = "/identifier/s";
    public static final String MAIN_TITLE_JSON_POINTER = "/entityDescription/m/mainTitle/s";
    public static final String TYPE_JSON_POINTER = "/entityDescription/m/reference/m/publicationInstance/m/type/s";
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBEventTransformer.class);
    public static final String MISSING_FIELD_LOGGER_WARNING_TEMPLATE =
            "The data from DynamoDB was incomplete, missing required field {} on id: {}, ignoring entry";
    public static final String TYPE = "type";
    public static final String TITLE = "title";

    /**
     * Creates a DynamoDBEventTransformer which creates a ElasticSearchIndexDocument from an dynamoDBEvent.
     */
    public DynamoDBEventTransformer() {
    }

    /**
     * Transforms a DynamoDB streamrecord into IndexDocument.
     * @param streamRecord of the original dynamoDB record
     * @return A document usable for indexing in elasticsearch
     */
    public IndexDocument parseStreamRecord(DynamodbEvent.DynamodbStreamRecord streamRecord) {
        JsonNode record = toJsonNode(streamRecord);

        String type = extractType(record);
        URI id = extractId(record);

        if (isNull(type)) {
            logger.warn(MISSING_FIELD_LOGGER_WARNING_TEMPLATE, TYPE, id);
            return null;
        }

        String title = extractTitle(record);

        if (isNull(title)) {
            logger.warn(MISSING_FIELD_LOGGER_WARNING_TEMPLATE, TITLE, id);
            return null;
        }

        return new IndexDocument.Builder()
                .withType(type)
                .withId(id)
                .withContributors(extractContributors(record))
                .withDate(new IndexDate(record))
                .withTitle(title)
                .build();
    }

    private List<IndexContributor> extractContributors(JsonNode record) {
        return toStream(record.at(CONTRIBUTOR_LIST_JSON_POINTER))
                .map(this::extractIndexContributor)
                .collect(Collectors.toList());
    }

    private IndexContributor extractIndexContributor(JsonNode jsonNode) {
        String identifier = textFromNode(jsonNode, CONTRIBUTOR_ARP_ID_JSON_POINTER);
        String name = textFromNode(jsonNode, CONTRIBUTOR_NAME_JSON_POINTER);
        return nonNull(name) ? generateIndexContributor(identifier, name) : null;
    }

    private URI extractId(JsonNode record) {
        return URI.create(Objects.requireNonNull(textFromNode(record, IDENTIFIER_JSON_POINTER)));
    }

    private String extractTitle(JsonNode record) {
        return textFromNode(record, MAIN_TITLE_JSON_POINTER);
    }

    private String extractType(JsonNode record) {
        return textFromNode(record, TYPE_JSON_POINTER);
    }

    private IndexContributor generateIndexContributor(String identifier, String name) {
        return new IndexContributor.Builder()
                .withId(identifier)
                .withName(name)
                .build();
    }

    private String textFromNode(JsonNode jsonNode, String jsonPointer) {
        JsonNode json = jsonNode.at(jsonPointer);
        return isPopulated(json) ? json.asText() : null;
    }

    private boolean isPopulated(JsonNode json) {
        return !json.isNull() && !json.asText().isBlank();
    }

    private JsonNode toJsonNode(DynamodbEvent.DynamodbStreamRecord streamRecord) {
        return mapper.valueToTree(streamRecord.getDynamodb().getNewImage());
    }

    private Stream<JsonNode> toStream(JsonNode contributors) {
        return StreamSupport.stream(contributors.spliterator(), false);
    }
}
