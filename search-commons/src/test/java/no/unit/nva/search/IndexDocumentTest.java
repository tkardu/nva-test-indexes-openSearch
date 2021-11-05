package no.unit.nva.search;

import com.fasterxml.jackson.databind.JsonNode;
import no.unit.nva.model.Publication;
import no.unit.nva.model.contexttypes.Publisher;
import no.unit.nva.model.exceptions.InvalidIsbnException;
import no.unit.nva.search.models.IndexDocument;
import no.unit.nva.search.utils.UriRetriever;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

import java.io.IOException;
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
import static no.unit.nva.search.models.IndexDocument.PUBLISHER_ID_JSON_PTR;
import static no.unit.nva.search.models.IndexDocument.SERIES_ID_JSON_PTR;
import static no.unit.nva.search.models.IndexDocument.fromPublication;
import static no.unit.nva.search.utils.IndexDocumentWrapperLinkedDataTest.PUBLISHER_NAME_JSON_PTR;
import static no.unit.nva.search.utils.IndexDocumentWrapperLinkedDataTest.SERIES_NAME_JSON_PTR;
import static no.unit.nva.search.utils.PublicationChannelGenerator.getPublicationChannelSampleJournal;
import static no.unit.nva.search.utils.PublicationChannelGenerator.getPublicationChannelSamplePublisher;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IndexDocumentTest {

    public static final Set<String> IGNORED_PUBLICATION_FIELDS = Set.of("doiRequest", "subjects");

    @Test
    public void toIndexDocumentCreatesReturnsNewIndexDocumentWithNoMissingFields() {
        Publication publication = publicationWithIdentifier();
        assertThat(publication, doesNotHaveEmptyValuesIgnoringFields(IGNORED_PUBLICATION_FIELDS));
        IndexDocument actualDocument = IndexDocument.fromPublication(publication);
        assertNotNull(actualDocument);
        assertTrue(actualDocument.hasPublicationType());
    }

    @Test
    void toJsonStringSerializesRequiredFields() throws InvalidIsbnException {
        Publication publication =
                sampleBookInABookSeriesWithAPublisher(randomPublicationChannelsUri(), publishingHouseWithUri());
        assertThat(publication, doesNotHaveEmptyValuesIgnoringFields(IGNORED_PUBLICATION_FIELDS));
        IndexDocument actualDocument = IndexDocument.fromPublication(publication);
        assertNotNull(actualDocument);
        assertNotNull(actualDocument.hasTitle());
        assertNotNull(actualDocument.hasPublicationType());
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
        assertTrue(actualDocument.toJsonString().contains("Degree"));
        assertTrue(StringUtils.isNotBlank(actualDocument.toString()));
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

    private static  UriRetriever mockPublicationChannelPublisherResponse(URI journalId,
                                                                         String journalName,
                                                                         URI publisherId,
                                                                         String publisherName)
            throws IOException, InterruptedException {
        final UriRetriever mockUriRetriever = mock(UriRetriever.class);
        String publicationChannelSampleJournal = getPublicationChannelSampleJournal(journalId, journalName);
        when(mockUriRetriever.getRawContent(eq(journalId), any())).thenReturn(publicationChannelSampleJournal);
        String publicationChannelSamplePublisher = getPublicationChannelSamplePublisher(publisherId, publisherName);
        when(mockUriRetriever.getRawContent(eq(publisherId), any())).thenReturn(publicationChannelSamplePublisher);
        return mockUriRetriever;
    }

}
