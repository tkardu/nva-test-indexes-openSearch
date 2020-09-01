package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import no.unit.nva.search.exception.InputException;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.handlers.ApiGatewayHandler;
import nva.commons.handlers.RequestInfo;
import nva.commons.handlers.RestRequestHandler;
import nva.commons.utils.Environment;
import org.slf4j.LoggerFactory;

public class SearchResourcesApiHandler extends ApiGatewayHandler<SearchResourcesRequest, SearchResourcesResponse> {


    public SearchResourcesApiHandler() {
        this(new Environment());
    }

    public SearchResourcesApiHandler(Environment environment) {
        super(SearchResourcesRequest.class, environment, LoggerFactory.getLogger(SearchResourcesApiHandler.class));
    }



    /**
     * Implements the main logic of the handler. Any exception thrown by this method will be handled by {@link
     * RestRequestHandler#handleExpectedException} method.
     *
     * @param input       The input object to the method. Usually a deserialized json.
     * @param requestInfo Request headers and path.
     * @param context     the ApiGateway context.
     * @return the Response body that is going to be serialized in json
     * @throws ApiGatewayException all exceptions are caught by writeFailure and mapped to error codes through the
     *                             method {@link RestRequestHandler#getFailureStatusCode}
     */
    @Override
    protected SearchResourcesResponse processInput(SearchResourcesRequest input,
                                                   RequestInfo requestInfo,
                                                   Context context) throws ApiGatewayException {
        throw new InputException("Nothing done here", new RuntimeException("Not yet made"));
    }

    /**
     * Define the success status code.
     *
     * @param input  The request input.
     * @param output The response output
     * @return the success status code.
     */
    @Override
    protected Integer getSuccessStatusCode(SearchResourcesRequest input, SearchResourcesResponse output) {
        return null;
    }
}
