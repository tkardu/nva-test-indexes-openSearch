package no.unit.nva.search;

import nva.commons.exceptions.ApiGatewayException;
import nva.commons.handlers.RequestInfo;
import nva.commons.utils.JacocoGenerated;

@JacocoGenerated
public class RequestUtil {

    public static final String SEARCH_TERM_KEY = "query";
    public static final String RESULTS_KEY = "results";
    private static final String RESULTS_DEFAULT_SIZE = "100";

    /**
     * Get searchTerm from request query parameters.
     *
     * @param requestInfo requestInfo
     * @return searchTerm given in query parameter
     * @throws ApiGatewayException exception containing explanatory message when parameter missing or inaccessible
     */
    public static String getSearchTerm(RequestInfo requestInfo) throws ApiGatewayException {
        return requestInfo.getQueryParameter(SEARCH_TERM_KEY);
    }

    public static String getResults(RequestInfo requestInfo) {
        return requestInfo.getQueryParameters().getOrDefault(RESULTS_KEY, RESULTS_DEFAULT_SIZE);
    }


}
