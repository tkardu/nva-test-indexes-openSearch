package no.unit.nva.search;

import com.fasterxml.jackson.databind.JsonNode;
import no.unit.nva.model.Publication;
import no.unit.nva.model.contexttypes.Publisher;
import no.unit.nva.model.exceptions.InvalidIsbnException;
import no.unit.nva.utils.UriRetriever;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Set;

import static no.unit.nva.hamcrest.DoesNotHaveEmptyValues.doesNotHaveEmptyValuesIgnoringFields;
import static no.unit.nva.publication.PublicationGenerator.publicationWithIdentifier;
import static no.unit.nva.publication.PublicationGenerator.publishingHouseWithUri;
import static no.unit.nva.publication.PublicationGenerator.randomPublicationChannelsUri;
import static no.unit.nva.publication.PublicationGenerator.randomString;
import static no.unit.nva.publication.PublicationGenerator.sampleBookInABookSeriesWithAPublisher;
import static no.unit.nva.publication.PublicationGenerator.sampleDegreeWithAPublisher;
import static no.unit.nva.publication.PublicationGenerator.sampleReportWithAPublisher;
import static no.unit.nva.search.IndexDocument.PUBLISHER_ID_JSON_PTR;
import static no.unit.nva.search.IndexDocument.SERIES_ID_JSON_PTR;
import static no.unit.nva.search.IndexDocument.fromPublication;
import static no.unit.nva.utils.IndexDocumentWrapperLinkedDataTest.PUBLISHER_NAME_JSON_PTR;
import static no.unit.nva.utils.IndexDocumentWrapperLinkedDataTest.SERIES_NAME_JSON_PTR;
import static no.unit.nva.utils.IndexDocumentWrapperLinkedDataTest.mockPublicationChannelPublisherResponse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
        assertDoesNotThrow(() -> actualDocument.getId().normalize());
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

    @Test
    public void fromPublicationReturnsIndexDocumnetWithValidReferenceData() throws Exception {
        final URI seriesUri = randomPublicationChannelsUri();
        final URI publisherUri = randomPublicationChannelsUri();
        final String publisherName = randomString();
        final String seriesName = randomString();
        final Publication publication = sampleBookInABookSeriesWithAPublisher(seriesUri, new Publisher(publisherUri));
        final UriRetriever mockUriRetriever =
                mockPublicationChannelPublisherResponse(seriesUri, seriesName, publisherUri, publisherName);
        final IndexDocument indexDocument = fromPublication(mockUriRetriever, publication);
        final JsonNode framedResultNode = indexDocument.asJsonNode();

        assertEquals(publisherUri.toString(), framedResultNode.at(PUBLISHER_ID_JSON_PTR).textValue());
        assertEquals(publisherName, framedResultNode.at(PUBLISHER_NAME_JSON_PTR).textValue());
        assertEquals(seriesUri.toString(), framedResultNode.at(SERIES_ID_JSON_PTR).textValue());
        assertEquals(seriesName, framedResultNode.at(SERIES_NAME_JSON_PTR).textValue());
    }
}
