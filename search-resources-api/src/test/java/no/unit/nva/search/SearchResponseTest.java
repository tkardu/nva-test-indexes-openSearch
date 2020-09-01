package no.unit.nva.search;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SearchResponseTest {

    @Test
    @DisplayName("testCreatingEmptySearchResponse")
    public void testCreatingEmptySearchResponse() {
        SearchResourcesResponse searchResourcesResponse = new SearchResourcesResponse();
        assertNotNull(searchResourcesResponse);
    }


}
