package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.search.IndexContributor;
import no.unit.nva.search.IndexDocument;
import nva.commons.utils.JsonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.nonNull;

public class DynamoDBEventTransformer {

    private static final ObjectMapper mapper = JsonUtils.objectMapper;

    public static final String YEAR_JSON_POINTER = "/entityDescription/m/date/m/year/s";
    public static final String MONTH_JSON_POINTER = "/entityDescription/m/date/m/month/s";
    public static final String DAY_JSON_POINTER = "/entityDescription/m/date/m/day/s";
    public static final String CONTRIBUTOR_LIST_JSON_POINTER = "/entityDescription/m/contributors/l";
    public static final String CONTRIBUTOR_ARP_ID_JSON_POINTER = "/m/identity/m/arpId/s";
    public static final String CONTRIBUTOR_NAME_JSON_POINTER = "/m/identity/m/name/s";
    public static final String IDENTIFIER_JSON_POINTER = "/identifier/s";
    public static final String MAIN_TITLE_JSON_POINTER = "/entityDescription/m/mainTitle/s";
    public static final String TYPE_JSON_POINTER = "/entityDescription/m/reference/m/publicationInstance/m/type/s";
    public static final String DATE_SEPARATOR = "-";

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

        return new IndexDocument.Builder()
                .withType(extractType(record))
                .withIdentifier(extractIdentifier(record))
                .withContributors(extractContributors(record))
                .withDate(extractDate(record))
                .withTitle(extractTitle(record))
                .build();
    }

    private String extractDate(JsonNode record) {
        return formatDate(extractYear(record), extractMonth(record), extractDay(record));
    }

    private String extractDay(JsonNode record) {
        return textFromNode(record, DAY_JSON_POINTER);
    }

    private String extractMonth(JsonNode record) {
        return textFromNode(record, MONTH_JSON_POINTER);
    }

    private String extractYear(JsonNode record) {
        return textFromNode(record, YEAR_JSON_POINTER);
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

    private String extractIdentifier(JsonNode record) {
        return textFromNode(record, IDENTIFIER_JSON_POINTER);
    }

    private String extractTitle(JsonNode record) {
        return textFromNode(record, MAIN_TITLE_JSON_POINTER);
    }

    private String extractType(JsonNode record) {
        return textFromNode(record, TYPE_JSON_POINTER);
    }

    private String formatDate(String year, String month, String day) {
        List<String> dateElements = new ArrayList<>();
        if (nonNull(year)) {
            dateElements.add(year);
        }
        if (valueIsPresentAndHasPrecedingValues(month, dateElements)) {
            dateElements.add(month);
        }
        if (valueIsPresentAndHasPrecedingValues(day, dateElements)) {
            dateElements.add(day);
        }
        return String.join(DATE_SEPARATOR, dateElements);
    }

    private boolean valueIsPresentAndHasPrecedingValues(String month, List<String> dateElements) {
        return nonNull(month) && !dateElements.isEmpty();
    }

    private IndexContributor generateIndexContributor(String identifier, String name) {
        return new IndexContributor.Builder()
                .withIdentifier(identifier)
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
