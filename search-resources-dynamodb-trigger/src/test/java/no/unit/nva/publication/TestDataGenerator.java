package no.unit.nva.publication;

import static no.unit.nva.publication.DynamoDBStreamHandler.REMOVE;
import static nva.commons.core.JsonUtils.objectMapper;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.file.Path;
import no.unit.nva.model.Publication;
import no.unit.nva.model.PublicationStatus;
import no.unit.nva.model.exceptions.InvalidIssnException;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.ioutils.IoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JacocoGenerated
@SuppressWarnings("PMD")
public class TestDataGenerator {

    public static final String UPDATE_TYPE_FIELD = "updateType";
    private static final Logger logger = LoggerFactory.getLogger(TestDataGenerator.class);
    private static final JsonPointer RESPONSE_PAYLOAD_POINTER =
        JsonPointer.compile("/detail/responsePayload");
    private static final String OLD_PUBLICATION_FIELD = "oldPublication";
    private static final String NEW_PUBLICATION_FIELD = "newPublication";
    private ObjectNode eventTemplate;
    private Publication oldPublication;
    private Publication newPublication;

    public TestDataGenerator() throws JsonProcessingException {
        initTemplate();
    }

    public Publication getOldPublication() {
        return oldPublication;
    }



    public Publication getNewPublication() {
        return newPublication;
    }

    public InputStream deletePublishedResourceEvent()
        throws JsonProcessingException, MalformedURLException, InvalidIssnException {
        initTemplate();
        Publication oldPublication = generateResource(PublicationStatus.PUBLISHED);
        addOldPublication(oldPublication);
        return toInputStream(eventTemplate);
    }

    public InputStream createResourceEvent(String eventType,
                                           PublicationStatus oldPublicationStatus,
                                           PublicationStatus newPublicationStatus)
        throws JsonProcessingException, MalformedURLException, InvalidIssnException {
        initTemplate();
        if (REMOVE.equals(eventType)) {
            return generateRemoveEvent(oldPublicationStatus);
        } else {
            return generateEvent(oldPublicationStatus, newPublicationStatus, eventType);
        }
    }

    private static ObjectNode emptyEventAsJsonNode() throws JsonProcessingException {
        String eventTemplate = IoUtils.stringFromResources(Path.of("resource_event_template.json"));
        return (ObjectNode) objectMapper.readTree(eventTemplate);
    }

    private void initTemplate() throws JsonProcessingException {
        eventTemplate = emptyEventAsJsonNode();
    }

    private InputStream generateRemoveEvent(PublicationStatus oldPublicationStatus)
        throws JsonProcessingException, MalformedURLException, InvalidIssnException {
        oldPublication = generateResource(oldPublicationStatus);
        oldPublication.setStatus(oldPublicationStatus);
        addOldPublication(oldPublication);
        return toInputStream(eventTemplate);
    }

    private InputStream generateEvent(PublicationStatus oldPublicationStatus,
                                      PublicationStatus newPublicationStatus,
                                      String eventType)
        throws JsonProcessingException, MalformedURLException, InvalidIssnException {
        oldPublication = generateResource(oldPublicationStatus);
        addOldPublication(oldPublication);
        newPublication = oldPublication.copy().withStatus(newPublicationStatus).build();
        addNewPublication(newPublication);
        updateEventType(eventType);

        return toInputStream(eventTemplate);
    }

    private void updateEventType(String eventType) {
        getResponsePayload().put(UPDATE_TYPE_FIELD, eventType);
    }

    private Publication generateResource(PublicationStatus publicationStatus)
        throws MalformedURLException, InvalidIssnException {
        Publication publication = PublicationGenerator.publicationWithIdentifier();
        publication.setStatus(publicationStatus);
        return publication;
    }

    private void addNewPublication(Publication newPublication) {
        addPublicationToResponsePayload(newPublication, NEW_PUBLICATION_FIELD);
    }

    private void addOldPublication(Publication oldPublication) {
        addPublicationToResponsePayload(oldPublication, OLD_PUBLICATION_FIELD);
    }

    private void addPublicationToResponsePayload(Publication newPublication, String newPublicationField) {
        JsonNode publicationNode = publicationToJsonNode(newPublication);
        ObjectNode responsePayload = getResponsePayload();
        responsePayload.set(newPublicationField, publicationNode);
    }

    private ObjectNode getResponsePayload() {
        return (ObjectNode) eventTemplate.at(RESPONSE_PAYLOAD_POINTER);
    }

    private InputStream toInputStream(ObjectNode objectNode) throws JsonProcessingException {
        String jsonString = objectMapper.writeValueAsString(objectNode);
        return IoUtils.stringToStream(jsonString);
    }

    private JsonNode publicationToJsonNode(Publication oldPublication) {
        return objectMapper.convertValue(oldPublication, JsonNode.class);
    }
}
