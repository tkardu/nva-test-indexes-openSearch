package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import no.unit.nva.search.exception.InputException;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.handlers.RequestInfo;
import nva.commons.utils.Environment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class SearchResourcesApiHandlerTest {



    @Test
    @DisplayName("testDefaultConstructor")
    public void testDefaultConstructor() {
        SearchResourcesApiHandler searchResourcesApiHandler = new SearchResourcesApiHandler();
        assertNotNull(searchResourcesApiHandler);
    }


    @Test
    @DisplayName("testConstructorWithParameters")
    public void testConstructorWithParameterEnvironmentDefined() {
        Environment environment = mock(Environment.class);
        SearchResourcesApiHandler searchResourcesApiHandler = new SearchResourcesApiHandler(environment);
        assertNotNull(searchResourcesApiHandler);
    }

    @Test
    @DisplayName("testProcessEmptyInputFails")
    public void testProcessEmptyInputReturnsNull() throws ApiGatewayException {
        Environment environment = mock(Environment.class);
        SearchResourcesApiHandler searchResourcesApiHandler = new SearchResourcesApiHandler(environment);
        assertNotNull(searchResourcesApiHandler);
        SearchResourcesRequest input = mock(SearchResourcesRequest.class);
        RequestInfo requestInfo = mock(RequestInfo.class);
        Context context = mock(Context.class);
        assertThrows(InputException.class, () ->  searchResourcesApiHandler.processInput(input, requestInfo, context));
    }

    @Test
    @DisplayName("testGetSuccessStatusCode")
    public void testGetSuccessStatusCode() {
        Environment environment = mock(Environment.class);
        SearchResourcesApiHandler searchResourcesApiHandler = new SearchResourcesApiHandler(environment);
        assertNotNull(searchResourcesApiHandler);
        SearchResourcesRequest request = mock(SearchResourcesRequest.class);
        SearchResourcesResponse response =  new SearchResourcesResponse();
        Integer statusCode = searchResourcesApiHandler.getSuccessStatusCode(request, response);
        assertNull(statusCode);
    }

    @Test
    @DisplayName("testCreatingEmptySearchRequest")
    public void testCreatingEmptySearchRequest() {
        SearchResourcesRequest searchResourcesRequest = new SearchResourcesRequest();
        assertNotNull(searchResourcesRequest);
    }

    @Test
    @DisplayName("testCreatingEmptySearchResponse")
    public void testCreatingEmptySearchResponse() {
        SearchResourcesResponse searchResourcesResponse = new SearchResourcesResponse();
        assertNotNull(searchResourcesResponse);
    }



}
