package no.unit.nva.publication;

import static no.unit.nva.model.PublicationStatus.PUBLISHED;
import static no.unit.nva.publication.PublicationUpdateEventHandler.REMOVE;
import static no.unit.nva.publication.PublicationGenerator.randomString;
import static nva.commons.core.JsonUtils.objectMapper;
import static nva.commons.core.attempt.Try.attempt;
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

@JacocoGenerated
@SuppressWarnings("PMD")
public class TestDataGenerator {

    public static final String UPDATE_TYPE_FIELD = "updateType";

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
        oldPublication = generateResource(PUBLISHED);
        newPublication =null;
        return toInputStream();
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

    public String createEmptyEvent() {
        return attempt(TestDataGenerator::emptyEventAsJsonNode)
                   .map(objectMapper::writeValueAsString)
                   .orElseThrow();
    }

    public InputStream createResourceWithNoInstance()
        throws MalformedURLException, InvalidIssnException, JsonProcessingException {
        oldPublication = generateResource(PUBLISHED);
        oldPublication.getEntityDescription()
            .getReference()
            .setPublicationInstance(null);
        newPublication = oldPublication.copy().build();
        newPublication.getEntityDescription().setMainTitle(randomString());
        return toInputStream();
    }

    public InputStream createResourceWithNoTitle()
        throws MalformedURLException, InvalidIssnException, JsonProcessingException {

        oldPublication = generateResource(PUBLISHED);
        newPublication = oldPublication.copy().build();
        newPublication.getEntityDescription().setMainTitle(null);

        return toInputStream();
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
        return toInputStream();
    }

    private InputStream generateEvent(PublicationStatus oldPublicationStatus,
                                      PublicationStatus newPublicationStatus,
                                      String eventType)
        throws JsonProcessingException, MalformedURLException, InvalidIssnException {
        oldPublication = generateResource(oldPublicationStatus);
        newPublication = oldPublication.copy().withStatus(newPublicationStatus).build();

        updateEventType(eventType);

        return toInputStream();
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

    private InputStream toInputStream() throws JsonProcessingException {
        addOldPublication(oldPublication);
        addNewPublication(newPublication);
        String jsonString = objectMapper.writeValueAsString(eventTemplate);
        return IoUtils.stringToStream(jsonString);
    }

    private JsonNode publicationToJsonNode(Publication oldPublication) {
        return objectMapper.convertValue(oldPublication, JsonNode.class);
    }
}
