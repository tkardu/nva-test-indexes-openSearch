package no.unit.nva.utils;

import no.unit.nva.search.IndexDocument;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.ioutils.IoUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@JacocoGenerated
public class IndexDocumentWrapperLinkedData {

    public static final String ACCEPT_LD_JSON = "application/ld+json";
    private static final String EMPTY_STRING = "";

    private final UriRetriever uriRetriever;

    public IndexDocumentWrapperLinkedData(UriRetriever uriRetriever) {
        this.uriRetriever = uriRetriever;
    }

    public String toFramedJsonLd(IndexDocument indexDocument) {
        try (InputStream frame = new SearchIndexFrame().asInputStream()) {
            return new FramedJsonGenerator(getInputStreams(indexDocument), frame).getFramedJson();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        return EMPTY_STRING;
    }

    private List<InputStream> getInputStreams(IndexDocument indexDocument) throws IOException, InterruptedException {
        final List<InputStream> inputStreams = new ArrayList<>();
        inputStreams.add(IoUtils.stringToStream(indexDocument.toJsonString()));
        inputStreams.add(fetch(indexDocument.getPublicationContextUri()));
        return inputStreams;
    }

    private InputStream fetch(URI externalReferences) {
        String data = getContent(externalReferences).orElseThrow();
        return IoUtils.stringToStream(data);
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
