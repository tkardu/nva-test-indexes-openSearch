package no.unit.nva.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.unit.nva.model.Publication;
import no.unit.nva.model.contexttypes.Book;
import no.unit.nva.model.contexttypes.BookSeries;
import no.unit.nva.model.contexttypes.Journal;
import no.unit.nva.model.contexttypes.Publisher;
import no.unit.nva.model.contexttypes.Series;
import no.unit.nva.model.exceptions.InvalidIsbnException;
import no.unit.nva.search.IndexDocument;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;

import static java.util.Map.entry;
import static no.unit.nva.publication.PublicationGenerator.publicationWithIdentifier;
import static no.unit.nva.publication.PublicationGenerator.randomPublicationChannelsUri;
import static no.unit.nva.publication.PublicationGenerator.randomString;
import static no.unit.nva.search.IndexDocument.PUBLISHER_ID_JSON_PTR;
import static no.unit.nva.search.IndexDocument.fromPublication;
import static nva.commons.core.JsonUtils.objectMapper;
import static nva.commons.core.ioutils.IoUtils.stringFromResources;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


class IndexDocumentWrapperLinkedDataTest {

    public static final String CONTEXT_TYPE_JSON_PTR = "/entityDescription/reference/publicationContext/type";
    public static final String NAME_JSON_PTR = "/entityDescription/reference/publicationContext/name";
    public static final String SERIES_ID_JSON_PTR = "/entityDescription/reference/publicationContext/id";
    public static final String JOURNAL_NAME = "Name Of The journal";
    public static final String CONTEXT_OBJECT_TYPE = "Journal";
    public static final String SAMPLE_JSON_FILENAME = "framed-json/publication_channel_sample.json";
    public static final String FIELD_NAME = "name";
    public static final String PUBLISHER_NAME_JSON_PTR =
            "/entityDescription/reference/publicationContext/publisher/name";
    public static final String FIELD_ID = "id";

    @Test
    public void toFramedJsonLdReturnsJsonWithValidReferenceData() throws Exception {

        final URI uri = randomPublicationChannelsUri();
        final UriRetriever journalResponse = mockPublicationChannelJournalResponse(uri);
        final IndexDocument indexDocument = generateIndexDocumentFromJournal(uri);
        final String framedJsonLd = new IndexDocumentWrapperLinkedData(journalResponse).toFramedJsonLd(indexDocument);
        JsonNode framedResultNode = objectMapper.readTree(framedJsonLd);

        assertEquals(uri.toString(), framedResultNode.at(SERIES_ID_JSON_PTR).textValue());
        assertEquals(JOURNAL_NAME, framedResultNode.at(NAME_JSON_PTR).textValue());
        assertEquals(CONTEXT_OBJECT_TYPE, framedResultNode.at(CONTEXT_TYPE_JSON_PTR).textValue());
    }

    @Test
    public void toFramedJsonLdReturnsJsonWithEnrichedPublisher() throws Exception {

        final URI journalId = randomPublicationChannelsUri();
        final URI publisherUri = randomPublicationChannelsUri();
        final String publisherName = randomString();

        final Publication publication = getPublicationBookWithLinkedContext(journalId, publisherUri);
        final IndexDocument indexDocument = fromPublication(publication);
        final UriRetriever publisherResponse =
                mockPublicationChannelPublisherResponse(journalId, publisherUri, publisherName);
        final String framedJsonLd = new IndexDocumentWrapperLinkedData(publisherResponse).toFramedJsonLd(indexDocument);
        final JsonNode framedResultNode = objectMapper.readTree(framedJsonLd);

        assertEquals(publisherUri.toString(), framedResultNode.at(PUBLISHER_ID_JSON_PTR).textValue());
        assertEquals(publisherName, framedResultNode.at(PUBLISHER_NAME_JSON_PTR).textValue());
    }

    private IndexDocument generateIndexDocumentFromJournal(URI journalId) {
        Publication publication = getPublicationJournalWithLinkedContext(journalId);
        return IndexDocument.fromPublication(publication);
    }

    private UriRetriever mockPublicationChannelJournalResponse(URI journalId) throws IOException, InterruptedException {
        final UriRetriever mockUriRetriever = mock(UriRetriever.class);
        String publicationChannelSample = getPublicationChannelSampleJournal(journalId);
        when(mockUriRetriever.getRawContent(eq(journalId), any())).thenReturn(publicationChannelSample);
        return mockUriRetriever;
    }

    private UriRetriever mockPublicationChannelPublisherResponse(URI journalId, URI publisherId, String publisherName)
            throws IOException, InterruptedException {
        final UriRetriever mockUriRetriever = mock(UriRetriever.class);
        String publicationChannelSampleJournal = getPublicationChannelSampleJournal(journalId);
        when(mockUriRetriever.getRawContent(eq(journalId), any())).thenReturn(publicationChannelSampleJournal);
        String publicationChannelSamplePublisher = getPublicationChannelSamplePublisher(publisherId, publisherName);
        when(mockUriRetriever.getRawContent(eq(publisherId), any())).thenReturn(publicationChannelSamplePublisher);
        return mockUriRetriever;
    }

    private String getPublicationChannelSampleJournal(URI journalId) throws JsonProcessingException {
        String publicationChannelSample = stringFromResources(Path.of(SAMPLE_JSON_FILENAME));
        JsonNode channelRoot = objectMapper.readTree(publicationChannelSample);

        ((ObjectNode) channelRoot).put(FIELD_ID, journalId.toString());
        ((ObjectNode) channelRoot).put(FIELD_NAME, JOURNAL_NAME);
        return objectMapper.writeValueAsString(channelRoot);
    }

    private String getPublicationChannelSamplePublisher(URI identifier, String publisherName)
            throws JsonProcessingException {
        Map<String, String> publisherMap = Map.ofEntries(
                entry("@context", "https://bibsysdev.github.io/src/publication-channel/channel-context.json"),
                entry("id", identifier.toString()),
                entry("name", publisherName));
        return objectMapper.writeValueAsString(publisherMap);
    }

    private Publication getPublicationJournalWithLinkedContext(URI journalId) {
        Publication publication = publicationWithIdentifier();
        publication.getEntityDescription().getReference().setPublicationContext(new Journal(journalId.toString()));
        return publication;
    }

    private Publication getPublicationBookWithLinkedContext(URI seriesId, URI publisherId) throws InvalidIsbnException {
        Publication publication = publicationWithIdentifier();
        final BookSeries bookSeries = new Series(seriesId);
        final Publisher publisher = new Publisher(publisherId);
        publication.getEntityDescription().getReference().setPublicationContext(
                new Book.BookBuilder().withSeries(bookSeries).withPublisher(publisher).build());
        return publication;
    }

}
