package no.unit.nva.utils;

import nva.commons.core.JacocoGenerated;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

@JacocoGenerated
public class UriRetriever {

    public String getRawContent(URI uri, String accept) throws IOException, InterruptedException {

        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .headers("Accept", accept)
                .GET()
                .build();

        HttpResponse<String> response = HttpClient
                .newBuilder()
                .build()
                .send(request, HttpResponse.BodyHandlers.ofString());

        return response.body();

    }
}
