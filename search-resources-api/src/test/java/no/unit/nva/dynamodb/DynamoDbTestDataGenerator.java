package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.unit.nva.search.IndexContributor;
import no.unit.nva.search.IndexDate;
import no.unit.nva.search.IndexDocument;
import nva.commons.utils.IoUtils;
import nva.commons.utils.JsonUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.Objects.nonNull;

public class DynamoDbTestDataGenerator {

    public static final String EVENT_TEMPLATE_JSON = "eventTemplate.json";
    public static final String CONTRIBUTOR_TEMPLATE_JSON = "contributorTemplate.json";

    public static final String EVENT_JSON_STRING_NAME = "s";
    public static final String EVENT_ID = "eventID";
    public static final String CONTRIBUTOR_SEQUENCE_POINTER = "/m/sequence";
    public static final String CONTRIBUTOR_NAME_POINTER = "/m/identity/m/name";
    public static final String CONTRIBUTOR_ARPID_POINTER = "/m/identity/m/arpId";
    public static final String CONTRIBUTOR_ID_POINTER = "/m/identity/m/id";
    public static final String CONTRIBUTOR_POINTER = "/records/0/dynamodb/newImage/entityDescription/m/contributors";
    public static final String EVENT_JSON_LIST_NAME = "l";

    public static final String ENTITY_DESCRIPTION_MAIN_TITLE_POINTER =
            "/records/0/dynamodb/newImage/entityDescription/m/mainTitle";
    public static final String PUBLICATION_INSTANCE_TYPE_POINTER =
            "/records/0/dynamodb/newImage/entityDescription/m/reference/m/publicationInstance/m/type";
    public static final String FIRST_RECORD_POINTER = "/records/0";
    public static final String EVENT_NAME = "eventName";

    public static final String IMAGE_IDENTIFIER_JSON_POINTER = "/records/0/dynamodb/newImage/identifier";
    public static final String ENTITY_DESCRIPTION_PUBLICATION_DATE_JSON_POINTER =
            "/records/0/dynamodb/newImage/entityDescription/m/date/m";
    public static final String EVENT_YEAR_NAME = "year";
    public static final String EVENT_MONTH_NAME = "month";
    public static final String EVENT_DAY_NAME = "day";

    private final ObjectMapper mapper = JsonUtils.objectMapper;
    private final JsonNode contributorTemplate = mapper.readTree(IoUtils.inputStreamFromResources(
            Paths.get(CONTRIBUTOR_TEMPLATE_JSON)));


    private final String eventId;
    private final String eventName;
    private final UUID id;
    private final String type;
    private final String mainTitle;
    private final List<Contributor> contributors;
    private final IndexDate date;

    private DynamoDbTestDataGenerator(Builder builder) throws IOException {
        eventId = builder.eventId;
        eventName = builder.eventName;
        id = builder.id;
        type = builder.type;
        mainTitle = builder.mainTitle;
        contributors = builder.contributors;
        date = builder.date;
    }

    /**
     * Provides a DynamodbEvent object representation of the object.
     * @return DynamodbEvent representation of the object.
     * @throws IOException thrown if the template files cannot be found.
     */
    public DynamodbEvent asDynamoDbEvent() throws IOException {
        ObjectNode event = getEventTemplate();
        updateEventImageIdentifier(id.toString(), event);
        updateEventId(eventId, event);
        updateEventName(eventName, event);
        updateReferenceType(type, event);
        updateEntityDescriptionMainTitle(mainTitle, event);
        updateEntityDescriptionContributors(contributors, event);
        updateDate(date, event);
        return toDynamodbEvent(event);
    }

    /**
     * Provides an IndexDocument representation of the object.
     * @return IndexDocument representation of object.
     */
    public IndexDocument asIndexDocument() {
        List<IndexContributor> indexContributors = new ArrayList<>();
        if (nonNull(contributors) && !contributors.isEmpty()) {
            contributors.forEach(contributor -> indexContributors.add(contributor.toIndexContributor()));
        }
        return new IndexDocument.Builder()
                .withId(id)
                .withType(type)
                .withTitle(mainTitle)
                .withContributors(indexContributors)
                .withDate(date)
                .build();
    }

    private ObjectNode getEventTemplate() throws IOException {
        return mapper.valueToTree(loadEventFromResourceFile());
    }

    private DynamodbEvent loadEventFromResourceFile() throws IOException {
        InputStream is = IoUtils.inputStreamFromResources(Paths.get(EVENT_TEMPLATE_JSON));
        return mapper.readValue(is, DynamodbEvent.class);
    }

    private DynamodbEvent toDynamodbEvent(JsonNode event) {
        return mapper.convertValue(event, DynamodbEvent.class);
    }

    private void updateEventImageIdentifier(String id, ObjectNode event) {
        updateEventAtPointerWithNameAndValue(event, IMAGE_IDENTIFIER_JSON_POINTER, EVENT_JSON_STRING_NAME, id);
    }

    private void updateEntityDescriptionContributors(List<Contributor> contributors, ObjectNode event) {
        ArrayNode contributorsArrayNode = mapper.createArrayNode();
        if (nonNull(contributors)) {
            contributors.forEach(contributor -> updateContributor(contributorsArrayNode, contributor));
            updateEventAtPointerWithNameAndArrayValue(event,
                    contributorsArrayNode);
            ((ObjectNode) event.at(CONTRIBUTOR_POINTER)).set(EVENT_JSON_LIST_NAME, contributorsArrayNode);
        }
    }

    private void updateContributor(ArrayNode contributors, Contributor contributor) {
        ObjectNode activeTemplate = contributorTemplate.deepCopy();
        updateEventAtPointerWithNameAndValue(activeTemplate, CONTRIBUTOR_SEQUENCE_POINTER,
                EVENT_JSON_STRING_NAME, contributor.getSequence());
        updateEventAtPointerWithNameAndValue(activeTemplate, CONTRIBUTOR_NAME_POINTER,
                EVENT_JSON_STRING_NAME, contributor.getName());
        updateEventAtPointerWithNameAndValue(activeTemplate, CONTRIBUTOR_ARPID_POINTER,
                EVENT_JSON_STRING_NAME, contributor.getArpId());
        updateEventAtPointerWithNameAndValue(activeTemplate, CONTRIBUTOR_ID_POINTER,
                EVENT_JSON_STRING_NAME, contributor.getId().toString());
        contributors.add(activeTemplate);
    }

    private void updateEntityDescriptionMainTitle(String mainTitle, ObjectNode event) {
        ((ObjectNode) event.at(ENTITY_DESCRIPTION_MAIN_TITLE_POINTER))
                .put(EVENT_JSON_STRING_NAME, mainTitle);
    }

    private void updateReferenceType(String type, ObjectNode event) {
        updateEventAtPointerWithNameAndValue(event, PUBLICATION_INSTANCE_TYPE_POINTER,
                EVENT_JSON_STRING_NAME, type);
    }

    private void updateEventId(String eventName, ObjectNode event) {
        ((ObjectNode) event.at(FIRST_RECORD_POINTER)).put(EVENT_ID, eventName);
    }

    private void updateEventName(String eventName, ObjectNode event) {
        ((ObjectNode) event.at(FIRST_RECORD_POINTER)).put(EVENT_NAME, eventName);
    }

    private void updateDate(IndexDate date, JsonNode event) {
        if (nonNull(date) && date.isPopulated()) {
            updateEventAtPointerWithNameAndValue(event, ENTITY_DESCRIPTION_PUBLICATION_DATE_JSON_POINTER,
                    EVENT_YEAR_NAME, date.getYear());
            updateEventAtPointerWithNameAndValue(event, ENTITY_DESCRIPTION_PUBLICATION_DATE_JSON_POINTER,
                    EVENT_MONTH_NAME, date.getMonth());
            updateEventAtPointerWithNameAndValue(event, ENTITY_DESCRIPTION_PUBLICATION_DATE_JSON_POINTER,
                    EVENT_DAY_NAME, date.getDay());
        }
    }

    private void updateEventAtPointerWithNameAndValue(JsonNode event, String pointer, String name, Object value) {
        if (value instanceof String) {
            ((ObjectNode) event.at(pointer)).put(name, (String) value);
        } else {
            ((ObjectNode) event.at(pointer)).put(name, (Integer) value);
        }
    }

    private void updateEventAtPointerWithNameAndArrayValue(ObjectNode event,
                                                           ArrayNode value) {
        ((ObjectNode) event.at(DynamoDbTestDataGenerator.CONTRIBUTOR_POINTER))
                .set(DynamoDbTestDataGenerator.EVENT_JSON_LIST_NAME, value);
    }

    public static final class Builder {
        private String eventId;
        private String eventName;
        private UUID id;
        private String type;
        private String mainTitle;
        private List<Contributor> contributors;
        private IndexDate date;

        public Builder() {
        }

        public Builder withEventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder withEventName(String eventName) {
            this.eventName = eventName;
            return this;
        }

        public Builder withId(UUID id) {
            this.id = id;
            return this;
        }

        public Builder withType(String type) {
            this.type = type;
            return this;
        }

        public Builder withMainTitle(String mainTitle) {
            this.mainTitle = mainTitle;
            return this;
        }

        public Builder withContributors(List<Contributor> contributors) {
            this.contributors = contributors;
            return this;
        }

        public Builder withDate(IndexDate date) {
            this.date = date;
            return this;
        }

        public DynamoDbTestDataGenerator build() throws IOException {
            return new DynamoDbTestDataGenerator(this);
        }
    }
}
