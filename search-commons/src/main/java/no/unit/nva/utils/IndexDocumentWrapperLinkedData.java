package no.unit.nva.utils;

import no.unit.nva.search.IndexDocument;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.ioutils.IoUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static nva.commons.apigateway.MediaTypes.APPLICATION_JSON_LD;
import static nva.commons.core.attempt.Try.attempt;

@JacocoGenerated
public class IndexDocumentWrapperLinkedData {

    private final UriRetriever uriRetriever;

    public IndexDocumentWrapperLinkedData(UriRetriever uriRetriever) {
        this.uriRetriever = uriRetriever;
    }

    public String toFramedJsonLd(IndexDocument indexDocument) throws IOException, InterruptedException {
        try (InputStream frame = new SearchIndexFrame().asInputStream()) {
            return new FramedJsonGenerator(getInputStreams(indexDocument), frame).getFramedJson();
        }
    }

    private List<InputStream> getInputStreams(IndexDocument indexDocument) throws IOException, InterruptedException {
        final List<InputStream> inputStreams = new ArrayList<>();
        inputStreams.add(IoUtils.stringToStream(indexDocument.toJsonString()));
        inputStreams.add(fetch(indexDocument.getPublicationContextUri()));
        return inputStreams;
    }

    private InputStream fetch(URI externalReferences) {
        return IoUtils.stringToStream(
                attempt(() -> uriRetriever.getRawContent(externalReferences, APPLICATION_JSON_LD.toString()))
                .orElseThrow());
    }
}
