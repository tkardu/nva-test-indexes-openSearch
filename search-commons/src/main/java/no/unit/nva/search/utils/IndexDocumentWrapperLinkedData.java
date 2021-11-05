package no.unit.nva.search.utils;

import com.fasterxml.jackson.databind.JsonNode;
import nva.commons.core.ioutils.IoUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static no.unit.nva.search.models.IndexDocument.getPublicationContextUris;
import static no.unit.nva.search.models.IndexDocument.toJsonString;
import static nva.commons.apigateway.MediaTypes.APPLICATION_JSON_LD;
import static nva.commons.core.ioutils.IoUtils.stringToStream;

public class IndexDocumentWrapperLinkedData {

    private final UriRetriever uriRetriever;

    public IndexDocumentWrapperLinkedData(UriRetriever uriRetriever) {
        this.uriRetriever = uriRetriever;
    }

    public String toFramedJsonLd(JsonNode indexDocument) throws IOException {
        try (InputStream frame = new SearchIndexFrame().asInputStream()) {
            return new FramedJsonGenerator(getInputStreams(indexDocument), frame).getFramedJson();
        }
    }

    private List<InputStream> getInputStreams(JsonNode indexDocument) {
        final List<InputStream> inputStreams = new ArrayList<>();
        inputStreams.add(stringToStream(toJsonString(indexDocument)));
        inputStreams.addAll(fetchAll(getPublicationContextUris(indexDocument)));
        return inputStreams;
    }

    private Collection<? extends InputStream> fetchAll(List<URI> publicationContextUris) {
        return publicationContextUris.stream()
                .map(this::fetch)
                .filter(Objects::nonNull)
                .map(IoUtils::stringToStream)
                .collect(Collectors.toList());
    }

    private String fetch(URI externalReference) {
        try {
            return uriRetriever.getRawContent(externalReference, APPLICATION_JSON_LD.toString());
        } catch (IOException | InterruptedException ignored) {
            return null;
        }
    }
}
