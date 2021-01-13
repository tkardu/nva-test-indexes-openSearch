package no.unit.nva.publication;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.unit.nva.search.IndexContributor;
import no.unit.nva.search.IndexDate;
import no.unit.nva.search.IndexDocument;
import no.unit.nva.search.IndexPublisher;
import nva.commons.utils.IoUtils;
import nva.commons.utils.JsonUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.Objects.nonNull;

public class TestDataGenerator {

    public static final String EVENT_TEMPLATE_JSON = "eventTemplate.json";
    public static final String CONTRIBUTOR_TEMPLATE_JSON = "contributorTemplate.json";
    private static final String DYNAMODB_STREAM_RECORD_SAMPLE_JSON = "sample_dynamodb_record.json";


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
    public static final String PUBLICATION_STATUS_JSON_POINTER = "/records/0/dynamodb/newImage/status";
    public static final String PUBLICATION_OWNER_JSON_POINTER = "/records/0/dynamodb/newImage/owner";
    public static final String PUBLICATION_MODIFIED_DATE_JSON_POINTER = "/records/0/dynamodb/newImage/modifiedDate";
    public static final String PUBLICATION_PUBLISHED_DATE_JSON_POINTER = "/records/0/dynamodb/newImage/publishedDate";

    public static final String ENTITY_DESCRIPTION_DESCRIPTION_JSON_POINTER =
            "/records/0/dynamodb/newImage/entityDescription/m/description";

    public static final String ENTITY_DESCRIPTION_ABSTRACT_JSON_POINTER =
            "/records/0/dynamodb/newImage/entityDescription/m/abstract";

    public static final String PUBLICATION_DOI_POINTER =
            "/records/0/dynamodb/newImage/entityDescription/m/reference/m/doi";

    public static final String PUBLISHER_ID_JSON_POINTER = "/records/0/dynamodb/newImage/publisher/m/id";
    public static final String PUBLISHER_TYPE_JSON_POINTER = "/records/0/dynamodb/newImage/publisher/m/type";
    private static final String ORGANIZATION_TYPE = "Organization";


    private final ObjectMapper mapper = JsonUtils.objectMapper;
    private final JsonNode contributorTemplate =
            mapper.readTree(IoUtils.inputStreamFromResources(CONTRIBUTOR_TEMPLATE_JSON));


    private final String eventId;
    private final String eventName;
    private final UUID id;
    private final URI doi;
    private final String type;
    private final String title;
    private final List<Contributor> contributors;
    private final IndexDate date;
    private final String status;
    private final String owner;
    private final String description;
    private final String publicationAbstract;
    private final IndexPublisher publisher;
    private final Instant modifiedDate;
    private final Instant publishedDate;

    private TestDataGenerator(Builder builder) throws IOException {
        eventId = builder.eventId;
        eventName = builder.eventName;
        id = builder.id;
        type = builder.type;
        title = builder.title;
        contributors = builder.contributors;
        date = builder.date;
        status = builder.status;
        owner = builder.owner;
        description = builder.description;
        publicationAbstract = builder.publicationAbstract;
        doi = builder.doi;
        publisher = builder.publisher;
        modifiedDate = builder.modifiedDate;
        publishedDate = builder.publishedDate;
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
        updateEntityDescriptionMainTitle(title, event);
        updateEntityDescriptionContributors(contributors, event);
        updateDate(date, event);
        updatePublicationStatus(status, event);
        updatePublicationOwner(owner, event);
        updatePublicationDescription(description, event);
        updatePublicationAbstract(publicationAbstract, event);
        updatePublisher(publisher, event);
        updateReferenceDoi(doi, event);
        updateModifiedDate(modifiedDate, event);
        updatePublishedDate(publishedDate, event);

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
                .withTitle(title)
                .withContributors(indexContributors)
                .withPublicationDate(date)
                .withOwner(owner)
                .withDescription(description)
                .withAbstract(publicationAbstract)
                .withDoi(doi)
                .withPublisher(publisher)
                .withModifiedDate(modifiedDate)
                .withPublishedDate(publishedDate)
                .build();
    }

    public JsonNode getSampleDynamoDBStreamRecord() throws IOException {
        return loadStreamRecordFromResourceFile();
    }

    private ObjectNode getEventTemplate() throws IOException {
        return mapper.valueToTree(loadEventFromResourceFile());
    }

    private DynamodbEvent loadEventFromResourceFile() throws IOException {
        InputStream is = IoUtils.inputStreamFromResources(EVENT_TEMPLATE_JSON);
        return mapper.readValue(is, DynamodbEvent.class);
    }

    private JsonNode loadStreamRecordFromResourceFile() throws IOException {
        InputStream is = IoUtils.inputStreamFromResources(DYNAMODB_STREAM_RECORD_SAMPLE_JSON);
        return mapper.readTree(is);
    }


    private DynamodbEvent toDynamodbEvent(JsonNode event) {
        return mapper.convertValue(event, DynamodbEvent.class);
    }

    private void updatePublicationStatus(String status, ObjectNode event) {
        updateEventAtPointerWithNameAndValue(event, PUBLICATION_STATUS_JSON_POINTER, EVENT_JSON_STRING_NAME, status);
    }

    private void updatePublicationOwner(String owner, ObjectNode event) {
        updateEventAtPointerWithNameAndValue(event, PUBLICATION_OWNER_JSON_POINTER, EVENT_JSON_STRING_NAME, owner);
    }

    private void updatePublicationDescription(String description, ObjectNode event) {
        updateEventAtPointerWithNameAndValue(event,
                ENTITY_DESCRIPTION_DESCRIPTION_JSON_POINTER,
                EVENT_JSON_STRING_NAME,
                description);
    }

    private void updatePublicationAbstract(String publicationAbstract, ObjectNode event) {
        updateEventAtPointerWithNameAndValue(event,
                ENTITY_DESCRIPTION_ABSTRACT_JSON_POINTER,
                EVENT_JSON_STRING_NAME,
                publicationAbstract);
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

    private void updateReferenceDoi(URI doi, ObjectNode event) {
        if (nonNull(doi)) {
            updateEventAtPointerWithNameAndValue(event, PUBLICATION_DOI_POINTER,
                    EVENT_JSON_STRING_NAME, doi.toString());
        }
    }

    private void updatePublisher(IndexPublisher publisher, ObjectNode event) {

        if (nonNull(publisher)) {
            updateEventAtPointerWithNameAndValue(event, PUBLISHER_ID_JSON_POINTER,
                    EVENT_JSON_STRING_NAME, publisher.getId().toString());
            updateEventAtPointerWithNameAndValue(event, PUBLISHER_TYPE_JSON_POINTER,
                    EVENT_JSON_STRING_NAME, ORGANIZATION_TYPE);
        }

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

    private void updateModifiedDate(Instant modifiedDate, ObjectNode event) {
        if (nonNull(modifiedDate)) {
            ((ObjectNode) event.at(PUBLICATION_MODIFIED_DATE_JSON_POINTER))
                    .put(EVENT_JSON_STRING_NAME, modifiedDate.toString());
        }
    }

    private void updatePublishedDate(Instant publishedDate, ObjectNode event) {
        if (nonNull(publishedDate)) {
            ((ObjectNode) event.at(PUBLICATION_PUBLISHED_DATE_JSON_POINTER))
                    .put(EVENT_JSON_STRING_NAME, publishedDate.toString());
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
        ((ObjectNode) event.at(TestDataGenerator.CONTRIBUTOR_POINTER))
                .set(TestDataGenerator.EVENT_JSON_LIST_NAME, value);
    }

    public static final class Builder {
        private String eventId;
        private String eventName;
        private UUID id;
        private URI doi;
        private String type;
        private String title;
        private String description;
        private String publicationAbstract;
        private String owner;
        private List<Contributor> contributors;
        private IndexDate date;
        private String status;
        private IndexPublisher publisher;
        private Instant modifiedDate;
        private Instant publishedDate;

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

        public Builder withDoi(URI doi) {
            this.doi = doi;
            return this;
        }

        public Builder withTitle(String title) {
            this.title = title;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder withAbstract(String publicationAbstract) {
            this.publicationAbstract = publicationAbstract;
            return this;
        }

        public Builder withOwner(String owner) {
            this.owner = owner;
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

        public Builder withStatus(String draft) {
            this.status = draft;
            return this;
        }

        public Builder withPublisher(IndexPublisher publisher) {
            this.publisher = publisher;
            return this;
        }

        public Builder withModifiedDate(Instant modifiedDate) {
            this.modifiedDate = modifiedDate;
            return this;
        }

        public Builder withPublishedDate(Instant publishedDate) {
            this.publishedDate = publishedDate;
            return this;
        }

        public TestDataGenerator build() throws IOException {
            return new TestDataGenerator(this);
        }
    }
}
