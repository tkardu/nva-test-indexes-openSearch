package no.unit.nva.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.unit.nva.model.Publication;
import no.unit.nva.model.contexttypes.Journal;
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
//    public static final String EXAMPLE_CHANNEL_JOURNAL_URI =
//            "https://api.dev.nva.aws.unit.no/publication-channels/journal/495273/2020";
    public static final String PUBLISHER_NAME_JSON_PTR =
            "/entityDescription/reference/publicationContext/publisher/name";

    @Test
    public void toFramedJsonLdReturnsJsonWithValidReferenceData() throws Exception {

        final URI uri = randomPublicationChannelsUri();
        final String framedJsonLd = new IndexDocumentWrapperLinkedData(getMockPublicationChannelJournalResponse(uri))
                .toFramedJsonLd(generateIndexDocumentFromJournal(uri));

        JsonNode framedResultNode = objectMapper.readTree(framedJsonLd);

        assertEquals(uri.toString(), framedResultNode.at(SERIES_ID_JSON_PTR).textValue());
        assertEquals(JOURNAL_NAME, framedResultNode.at(NAME_JSON_PTR).textValue());
        assertEquals(CONTEXT_OBJECT_TYPE, framedResultNode.at(CONTEXT_TYPE_JSON_PTR).textValue());
    }

    @Test
    public void toFramedJsonLdReturnsJsonWithEnrichedPublisher() throws Exception {

        final URI journalId = randomPublicationChannelsUri();
        final URI publisherUri = randomPublicationChannelsUri();
        String publisherName = randomString();
        final IndexDocument indexDocument = generateIndexDocumentFromJournalWithPublisher(journalId, publisherUri);
        final UriRetriever channelPublisherResponse = getMockPublicationChannelPublisherResponse(publisherUri, publisherName);
        final String framedJsonLd = new IndexDocumentWrapperLinkedData(channelPublisherResponse)
                .toFramedJsonLd(indexDocument);

        JsonNode framedResultNode = objectMapper.readTree(framedJsonLd);

        assertEquals(publisherUri.toString(), framedResultNode.at(PUBLISHER_ID_JSON_PTR).textValue());
        assertEquals(publisherName, framedResultNode.at(PUBLISHER_NAME_JSON_PTR).textValue());
    }



    private IndexDocument generateIndexDocumentFromJournal(URI journalId) {
        Publication publication = getPublicationJournalWithLinkedContext(journalId);
        return IndexDocument.fromPublication(publication);
    }

    private UriRetriever getMockPublicationChannelJournalResponse(URI journalId) throws IOException, InterruptedException {
        final UriRetriever mockUriRetriever = mock(UriRetriever.class);
        String publicationChannelSample = getPublicationChannelSampleJournal();
        when(mockUriRetriever.getRawContent(eq(journalId), any())).thenReturn(publicationChannelSample);
        return mockUriRetriever;
    }

    private UriRetriever getMockPublicationChannelPublisherResponse(URI publisherId, String publisherName) throws IOException, InterruptedException {
        final UriRetriever mockUriRetriever = mock(UriRetriever.class);
        String publicationChannelSample = getPublicationChannelSamplePublisher(publisherId, publisherName);
        when(mockUriRetriever.getRawContent(eq(publisherId), any())).thenReturn(publicationChannelSample);
        return mockUriRetriever;
    }



    private String getPublicationChannelSampleJournal() throws JsonProcessingException {
        String publicationChannelSample = stringFromResources(Path.of(SAMPLE_JSON_FILENAME));
        JsonNode channelRoot = objectMapper.readTree(publicationChannelSample);
        ((ObjectNode) channelRoot).put(FIELD_NAME, JOURNAL_NAME);
        return objectMapper.writeValueAsString(channelRoot);
    }

    private String getPublicationChannelSamplePublisher(URI identifier, String publisherName) throws JsonProcessingException {
        Map<String, String> publisherMap = Map.ofEntries(
                entry("id", identifier.toString()),
                entry("name", publisherName));
        return objectMapper.writeValueAsString(publisherMap);
    }

    private Publication getPublicationJournalWithLinkedContext(URI journalId) {
        Publication publication = publicationWithIdentifier();
        publication.getEntityDescription().getReference().setPublicationContext(new Journal(journalId.toString()));
        return publication;
    }


    private Publication getPublicationJournalWithLinkedContextAndPublisher(URI journalId, URI publisherId) {
        Publication publication = publicationWithIdentifier();
        final Journal journal = new Journal(journalId.toString());
        publication.getEntityDescription().getReference().setPublicationContext(journal);

        return publication;
    }

    private IndexDocument generateIndexDocumentFromJournalWithPublisher(URI journalId, URI publisherId) throws JsonProcessingException {
        final String PUBLICATION_CONTEXT_JSON_PTR = "/entityDescription/reference/publicationContext";
        final Publication publication = getPublicationJournalWithLinkedContext(journalId);

        JsonNode publicationRoot = objectMapper.valueToTree(publication);
        final ObjectNode publicationContextNode = (ObjectNode) publicationRoot.at(PUBLICATION_CONTEXT_JSON_PTR);
        ObjectNode publisher = objectMapper.createObjectNode().put("id", publisherId.toString()).put("type", "publisher");
        publicationContextNode.set("publisher", publisher);

        return fromPublication(objectMapper.treeToValue(publicationRoot, Publication.class));
    }

}
