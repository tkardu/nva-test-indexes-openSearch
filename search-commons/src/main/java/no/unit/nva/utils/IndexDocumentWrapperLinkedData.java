package no.unit.nva.utils;

import no.unit.nva.search.IndexDocument;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.ioutils.IoUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static nva.commons.apigateway.MediaTypes.APPLICATION_JSON_LD;
import static nva.commons.core.attempt.Try.attempt;

@JacocoGenerated
public class IndexDocumentWrapperLinkedData {

    private final UriRetriever uriRetriever;

    public IndexDocumentWrapperLinkedData(UriRetriever uriRetriever) {
        this.uriRetriever = uriRetriever;
    }

    public String toFramedJsonLd(IndexDocument indexDocument) throws IOException {
        try (InputStream frame = new SearchIndexFrame().asInputStream()) {
            return new FramedJsonGenerator(getInputStreams(indexDocument), frame).getFramedJson();
        }
    }

    private List<InputStream> getInputStreams(IndexDocument indexDocument) {
        final List<InputStream> inputStreams = new ArrayList<>();
        inputStreams.add(IoUtils.stringToStream(indexDocument.toJsonString()));
        inputStreams.addAll(fetchAll(indexDocument.getPublicationContextUris()));
        return inputStreams;
    }

    private Collection<? extends InputStream> fetchAll(List<URI> publicationContextUris) {
        return publicationContextUris.stream()
                .map(this::fetch)
                .collect(Collectors.toList());
    }


    private InputStream fetch(URI externalReference) {
        return IoUtils.stringToStream(
                attempt(() -> uriRetriever.getRawContent(externalReference, APPLICATION_JSON_LD.toString()))
                .orElseThrow());
    }
}
