package no.unit.nva.publication;

import static nva.commons.core.JsonUtils.objectMapper;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.nio.file.Path;
import no.unit.nva.model.Publication;
import no.unit.nva.model.PublicationStatus;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.ioutils.IoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JacocoGenerated
@SuppressWarnings("PMD")
public class TestDataGenerator {

    private static final Logger logger = LoggerFactory.getLogger(TestDataGenerator.class);
    private static final JsonPointer RESPONSE_PAYLOAD_POINTER =
        JsonPointer.compile("/detail/responsePayload");
    private static final String OLD_PUBLICATION_FIELD = "oldPublication";
    private static final String NEW_PUBLICATION_FIELD ="newPublication";


   public static InputStream emptyEvent() throws JsonProcessingException {
       String eventString = objectMapper.writeValueAsString(emptyEventAsJsonNode());
       return IoUtils.stringToStream(eventString);
   }

   public static InputStream deletePublishedResourceEvent() throws JsonProcessingException {
       ObjectNode template = emptyEventAsJsonNode();
       Publication oldPublication = publishedResource();
       addOldPublicationToTemplate(template, oldPublication);
       return toInputStream(template);
   }

    private static Publication publishedResource() {
        Publication oldPublication = PublicationGenerator.publicationWithIdentifier();
        oldPublication.setStatus(PublicationStatus.PUBLISHED);
        return oldPublication;
    }

    private static void addOldPublicationToTemplate(ObjectNode template, Publication oldPublication) {
        JsonNode oldPublicationNode = publicationToJsonNode(oldPublication);
        ObjectNode responsePayload= (ObjectNode) template.at(RESPONSE_PAYLOAD_POINTER);
        responsePayload.set(OLD_PUBLICATION_FIELD,oldPublicationNode);
    }

    private static InputStream toInputStream(ObjectNode objectNode) throws JsonProcessingException {
        String jsonString = objectMapper.writeValueAsString(objectNode);
        return IoUtils.stringToStream(jsonString);
    }

    private static JsonNode publicationToJsonNode(Publication oldPublication) {
        return objectMapper.convertValue(oldPublication, JsonNode.class);
    }

    private static ObjectNode emptyEventAsJsonNode() throws JsonProcessingException {
        String eventTemplate = IoUtils.stringFromResources(Path.of("resource_event_template.json"));
        return (ObjectNode) objectMapper.readTree(eventTemplate);

    }
}
