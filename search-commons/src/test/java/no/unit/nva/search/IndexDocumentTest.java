package no.unit.nva.search;

import no.unit.nva.model.Publication;
import no.unit.nva.model.contexttypes.Publisher;
import no.unit.nva.model.exceptions.InvalidIsbnException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Set;

import static no.unit.nva.hamcrest.DoesNotHaveEmptyValues.doesNotHaveEmptyValuesIgnoringFields;
import static no.unit.nva.publication.PublicationGenerator.sampleBookInABookSeriesWithAPublisher;
import static no.unit.nva.publication.PublicationGenerator.sampleDegreeWithAPublisher;
import static no.unit.nva.publication.PublicationGenerator.sampleReportWithAPublisher;
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
        Publication publication =
                sampleBookInABookSeriesWithAPublisher(randomPublicationChannelsUri(), publishingHouseWithUri());
        assertThat(publication, doesNotHaveEmptyValuesIgnoringFields(IGNORED_PUBLICATION_FIELDS));
        IndexDocument actualDocument = IndexDocument.fromPublication(publication);
        assertNotNull(actualDocument);
    }

    @Test
    public void fromPublicationAndBack() throws Exception {
        Publication publication =
                sampleBookInABookSeriesWithAPublisher(randomPublicationChannelsUri(), publishingHouseWithUri());
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
        Publication publication =
                sampleBookInABookSeriesWithAPublisher(randomPublicationChannelsUri(), publisher);
        IndexDocument actualDocument = IndexDocument.fromPublication(publication);
        assertTrue(actualDocument.hasPublicationType());
        assertTrue(actualDocument.getPublicationContextUris().contains(publisherId));
    }

    @Test
    public void getPublicationContextUrisReturnsSeriesIdWhenBookSeriesHasPublicationChannelId() throws Exception {
        final Publisher publisher = new Publisher(randomPublicationChannelsUri());
        final URI bookSeriesUri = randomPublicationChannelsUri();
        Publication publication = sampleBookInABookSeriesWithAPublisher(bookSeriesUri, publisher);
        IndexDocument actualDocument = IndexDocument.fromPublication(publication);
        assertEquals("Book", actualDocument.getPublicationContextType());
        assertTrue(actualDocument.getPublicationContextUris().contains(bookSeriesUri));
    }

    @Test
    public void getPublicationContextUrisReturnsSeriesIdWhenDegreeIsPartOfSeriesWithPublicationChannelId()
            throws Exception {
        final Publisher publisher = new Publisher(randomPublicationChannelsUri());
        final URI bookSeriesUri = randomPublicationChannelsUri();
        Publication publication = sampleDegreeWithAPublisher(bookSeriesUri, publisher);
        IndexDocument actualDocument = IndexDocument.fromPublication(publication);
        assertEquals("Degree", actualDocument.getPublicationContextType());
        assertTrue(actualDocument.getPublicationContextUris().contains(bookSeriesUri));
    }

    @Test
    public void getPublicationContextUrisReturnsSeriesIdWhenReportIsPartOfSeriesWithPublicationChannelId()
            throws Exception {
        final Publisher publisher = new Publisher(randomPublicationChannelsUri());
        final URI bookSeriesUri = randomPublicationChannelsUri();
        Publication publication = sampleReportWithAPublisher(bookSeriesUri, publisher);
        IndexDocument actualDocument = IndexDocument.fromPublication(publication);
        assertEquals("Report", actualDocument.getPublicationContextType());
        assertTrue(actualDocument.getPublicationContextUris().contains(bookSeriesUri));
    }
}
