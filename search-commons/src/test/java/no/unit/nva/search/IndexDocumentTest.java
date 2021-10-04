package no.unit.nva.search;

import no.unit.nva.model.EntityDescription;
import no.unit.nva.model.Publication;
import no.unit.nva.model.contexttypes.Publisher;
import no.unit.nva.model.contexttypes.PublishingHouse;
import no.unit.nva.model.exceptions.InvalidIsbnException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Set;

import static no.unit.nva.hamcrest.DoesNotHaveEmptyValues.doesNotHaveEmptyValuesIgnoringFields;
import static no.unit.nva.publication.PublicationGenerator.createPublicationWithEntityDescription;
import static no.unit.nva.publication.PublicationGenerator.createSampleEntityDescriptionBook;
import static no.unit.nva.publication.PublicationGenerator.publicationWithIdentifier;
import static no.unit.nva.publication.PublicationGenerator.publishingHouseWithUri;
import static no.unit.nva.publication.PublicationGenerator.randomPublicationChannelsUri;
import static nva.commons.core.JsonUtils.objectMapper;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexDocumentTest {

    public static final Set<String> IGNORED_PUBLICATION_FIELDS = Set.of("doiRequest", "subjects");

    @Test
    public void toIndexDocumentCreatesReturnsNewIndexDocumentWithNoMissingFields() {
        Publication publication = publicationWithIdentifier();
        assertThat(publication, doesNotHaveEmptyValuesIgnoringFields(IGNORED_PUBLICATION_FIELDS));
        IndexDocument actualDocument = IndexDocument.fromPublication(publication);
        assertNotNull(actualDocument);
    }

    @Test
    void toJsonStringSerializesRequiredFields() throws InvalidIsbnException {
        Publication publication = createSampleBookInABookSeriesFromAPublisher(publishingHouseWithUri());
        assertThat(publication, doesNotHaveEmptyValuesIgnoringFields(IGNORED_PUBLICATION_FIELDS));
        IndexDocument actualDocument = IndexDocument.fromPublication(publication);
        assertNotNull(actualDocument);
    }

    @Test
    public void fromPublicationAndBack() throws Exception {
        Publication publication = createSampleBookInABookSeriesFromAPublisher(publishingHouseWithUri());
        IndexDocument actualDocument = IndexDocument.fromPublication(publication);
        final String jsonString = actualDocument.toJsonString();
        assertNotNull(jsonString);
        final Publication restoredPublication = objectMapper.readValue(jsonString, Publication.class);
        assertNotNull(restoredPublication);
        assertEquals(publication, restoredPublication);
    }

    @Test
    public void getPublicationContextUrisReturnsPublisherIdWhenPublisherHasPublicationChannelId() throws Exception {
        URI publisherId = randomPublicationChannelsUri();
        final Publisher publisher = new Publisher(publisherId);
        Publication publication = createSampleBookInABookSeriesFromAPublisher(publisher);
        IndexDocument actualDocument = IndexDocument.fromPublication(publication);
        assertTrue(actualDocument.hasPublicationType());
        assertTrue(actualDocument.getPublicationContextUris().contains(publisherId));
    }

    private Publication createSampleBookInABookSeriesFromAPublisher(PublishingHouse publishingHouse)
            throws InvalidIsbnException {
        EntityDescription entityDescription =
                createSampleEntityDescriptionBook(randomPublicationChannelsUri(), publishingHouse);
        return createPublicationWithEntityDescription(entityDescription);
    }

}
