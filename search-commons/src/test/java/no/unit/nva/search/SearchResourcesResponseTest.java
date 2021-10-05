package no.unit.nva.search;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class SearchResourcesResponseTest {

    public static final URI EXAMPLE_CONTEXT = URI.create("https://example.org/search");
    public static final URI EXAMPLE_ID = URI.create("https://example.org/search?query=someIdentifier");
    public static final List<JsonNode> SAMPLE_HITS = Collections.EMPTY_LIST;
    public static final int SAMPLE_TOOK = 0;
    public static final int SAMPLE_TOTAL = 0;

    @Test
    void getSuccessStatusCodeReturnsOK() {
        SearchResourcesResponse response =  new SearchResourcesResponse(EXAMPLE_CONTEXT,
                EXAMPLE_ID,
                SAMPLE_TOOK,
                SAMPLE_TOTAL,
                SAMPLE_HITS);
        assertNotNull(response);
    }
}
