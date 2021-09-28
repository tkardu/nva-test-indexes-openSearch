package no.unit.nva.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.unit.nva.model.Publication;
import no.unit.nva.model.contexttypes.Journal;
import no.unit.nva.publication.PublicationGenerator;
import no.unit.nva.utils.IndexDocumentWrapperLinkedData;
import no.unit.nva.utils.PublicationChannelsFilter;
import no.unit.nva.utils.UriRetriever;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Path;

import static nva.commons.core.JsonUtils.objectMapper;
import static nva.commons.core.ioutils.IoUtils.stringFromResources;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


class IndexDocumentWrapperLinkedDataTest {

    public static final String TYPE_JSON_PTR = "/entityDescription/reference/publicationContext/type";
    public static final String NAME_JSON_PTR = "/entityDescription/reference/publicationContext/name";
    public static final String SERIES_ID_JSON_PTR = "/entityDescription/reference/publicationContext/id";
    public static final String JOURNAL_NAME = "name of the journal";
    public static final String CONTEXT_OBJECT_TYPE = "Journal";
    public static final String SAMPLE_JSON_FILENAME = "framed-json/publication_channel_sample.json";
    public static final String FIELD_NAME = "name";

    @Test
    public void toFramedJsonLdProducesJsonLdIncludingReferencedData() throws Exception {

        final URI uri = getRandomUri();
        Publication publication = getPublicationWithLinkedContext(uri);
        IndexDocument indexDocument = IndexDocument.fromPublication(publication);
        final UriRetriever mockUriRetriever = mock(UriRetriever.class);

        String publicationChannelSample = getPublicationChannelSample(JOURNAL_NAME);
        when(mockUriRetriever.getRawContent(any(), any())).thenReturn(publicationChannelSample);
        final String framedJsonLd = new IndexDocumentWrapperLinkedData(mockUriRetriever,
                PublicationChannelsFilter::isPublicationChannelUri).toFramedJsonLd(indexDocument);
        assertNotNull(framedJsonLd);
        JsonNode framedResultNode = objectMapper.readTree(framedJsonLd);
        assertNotNull(framedResultNode);

        assertEquals(framedResultNode.at(SERIES_ID_JSON_PTR).textValue(), uri.toString());
        assertEquals(framedResultNode.at(NAME_JSON_PTR).textValue(), JOURNAL_NAME);
        assertEquals(framedResultNode.at(TYPE_JSON_PTR).textValue(), CONTEXT_OBJECT_TYPE);
    }

    private String getPublicationChannelSample(String name) throws JsonProcessingException {
        String publicationChannelSample = stringFromResources(Path.of(SAMPLE_JSON_FILENAME));
        JsonNode channelRoot = objectMapper.readTree(publicationChannelSample);
        ((ObjectNode) channelRoot).put(FIELD_NAME, name);
        return objectMapper.writeValueAsString(channelRoot);
    }

    private Publication getPublicationWithLinkedContext(URI uri) {
        Publication publication = PublicationGenerator.publicationWithIdentifier();
        publication.getEntityDescription().getReference().setPublicationContext(new Journal(uri.toString()));
        return publication;
    }

    private URI getRandomUri() {
        return URI.create("https://api.dev.nva.aws.unit.no/publication-channels/journal/495273/2020");
    }


}
