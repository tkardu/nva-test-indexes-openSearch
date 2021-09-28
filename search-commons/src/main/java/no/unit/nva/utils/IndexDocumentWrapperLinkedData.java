package no.unit.nva.utils;

import no.unit.nva.search.IndexDocument;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.ioutils.IoUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@JacocoGenerated
public class IndexDocumentWrapperLinkedData {


    public static final String ACCEPT_LD_JSON = "application/ld+json";
    private static final String EMPTY_STRING = "";

    private final UriRetriever uriRetriever;
    private final Predicate<URI> uriFilter;

    public IndexDocumentWrapperLinkedData() {
        this(new UriRetriever(), PublicationChannelsFilter::isPublicationChannelUri);
    }

    public IndexDocumentWrapperLinkedData(UriRetriever uriRetriever, Predicate<URI> uriFilter) {
        this.uriRetriever = uriRetriever;
        this.uriFilter = uriFilter;
    }


    @SuppressWarnings("PMD.CloseResource")
    public String toFramedJsonLd(IndexDocument indexDocument) {
        try {

            final String indexDocumentJson = indexDocument.toJsonLdString();
            final InputStream publicationSource = IoUtils.stringToStream(indexDocumentJson);
            final List<InputStream> objects = new ArrayList<>(Arrays.asList(publicationSource));
            objects.addAll(getInputStreams(indexDocument.getIds()));

            InputStream frame = new SearchIndexFrame().asInputStream();
            var framedJson = new FramedJsonGenerator(objects, frame);
            return framedJson.getFramedJson();

        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        return EMPTY_STRING;
    }


    private List<InputStream> getInputStreams(List<URI> externalReferences) throws IOException, InterruptedException {
        List<InputStream> objects = externalReferences.stream()
                .filter(uriFilter)
                .map(this::getContent)
                .map(Optional::get)
                .map(IoUtils::stringToStream)
                .collect(Collectors.toList());
        return objects;
    }

    private Optional<String> getContent(URI uri) {
        try {
            return Optional.ofNullable(uriRetriever.getRawContent(uri, ACCEPT_LD_JSON));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }
}
