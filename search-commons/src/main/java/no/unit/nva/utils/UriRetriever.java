package no.unit.nva.utils;

import nva.commons.core.JacocoGenerated;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;

import static java.net.http.HttpResponse.BodyHandlers.ofString;

@JacocoGenerated
public class UriRetriever {

    public static final String ACCEPT = "Accept";

    public String getRawContent(URI uri, String mediaType) throws IOException, InterruptedException {
        return HttpClient
                .newBuilder()
                .build()
                .send(createHttpRequest(uri, mediaType), ofString())
                .body();
    }

    private HttpRequest createHttpRequest(URI uri, String mediaType) {
        return HttpRequest.newBuilder()
                .uri(uri)
                .headers(ACCEPT, mediaType)
                .GET()
                .build();
    }
}
