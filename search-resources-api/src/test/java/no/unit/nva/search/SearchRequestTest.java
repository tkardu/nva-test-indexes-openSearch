package no.unit.nva.search;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SearchRequestTest {

    @Test
    @DisplayName("testCreatingEmptySearchRequest")
    public void testCreatingEmptySearchRequest() {
        SearchResourcesRequest searchResourcesRequest = new SearchResourcesRequest();
        assertNotNull(searchResourcesRequest);
    }

}
