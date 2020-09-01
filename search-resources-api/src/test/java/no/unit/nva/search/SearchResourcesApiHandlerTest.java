package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import no.unit.nva.search.exception.InputException;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.handlers.RequestInfo;
import nva.commons.utils.Environment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class SearchResourcesApiHandlerTest {

    private Environment environment;
    private SearchResourcesApiHandler searchResourcesApiHandler;

    @BeforeEach
    public void init() {
        environment = mock(Environment.class);
        searchResourcesApiHandler = new SearchResourcesApiHandler(environment);
    }

    @Test
    @DisplayName("testDefaultConstructor")
    public void testDefaultConstructor() {
        SearchResourcesApiHandler searchResourcesApiHandler = new SearchResourcesApiHandler();
        assertNotNull(searchResourcesApiHandler);
    }

    @Test
    @DisplayName("testProcessEmptyInputFails")
    public void testProcessEmptyInputReturnsNull() throws ApiGatewayException {
        SearchResourcesRequest input = mock(SearchResourcesRequest.class);
        RequestInfo requestInfo = mock(RequestInfo.class);
        Context context = mock(Context.class);
        assertThrows(InputException.class, () ->  searchResourcesApiHandler.processInput(input, requestInfo, context));
    }

    @Test
    @DisplayName("testGetSuccessStatusCode")
    public void testGetSuccessStatusCode() {
        SearchResourcesRequest request = mock(SearchResourcesRequest.class);
        SearchResourcesResponse response =  new SearchResourcesResponse();
        Integer statusCode = searchResourcesApiHandler.getSuccessStatusCode(request, response);
        assertNull(statusCode);
    }

}
