package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import no.unit.nva.search.exception.InputException;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.handlers.RequestInfo;
import nva.commons.utils.Environment;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    public void testDefaultConstructor() {
        SearchResourcesApiHandler searchResourcesApiHandler = new SearchResourcesApiHandler();
        assertNotNull(searchResourcesApiHandler);
    }

    @Test
    public void processInputReturnsNullWhenInputIsEmpty() throws ApiGatewayException {
        SearchResourcesRequest input = mock(SearchResourcesRequest.class);
        RequestInfo requestInfo = mock(RequestInfo.class);
        Context context = mock(Context.class);
        assertThrows(InputException.class, () ->  searchResourcesApiHandler.processInput(input, requestInfo, context));
    }

    @Test
    public void getSuccessStatusCodeReturnsOK() {
        SearchResourcesRequest request = new  SearchResourcesRequest();
        SearchResourcesResponse response =  new SearchResourcesResponse();
        Integer statusCode = searchResourcesApiHandler.getSuccessStatusCode(request, response);
        assertEquals(statusCode, HttpStatus.SC_OK);
    }

}
