package no.unit.nva.search;

import no.unit.nva.search.models.Query;
import nva.commons.apigateway.RequestInfo;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.paths.UriWrapper;
import org.elasticsearch.search.sort.SortOrder;

import java.net.URI;

import static nva.commons.core.attempt.Try.attempt;

@JacocoGenerated
public class RequestUtil {

    public static final String SEARCH_TERM_KEY = "query";
    public static final String RESULTS_KEY = "results";
    public static final String FROM_KEY = "from";
    public static final String ORDERBY_KEY = "orderBy";
    public static final String SORTORDER_KEY = "sortOrder";
    private static final String RESULTS_DEFAULT_SIZE = "10";
    public static final String SEARCH_ALL_PUBLICATIONS_DEFAULT_QUERY = "*";
    private static final String ORDERBY_DEFAULT_POSITION = "modifiedDate";
    private static final String DEFAULT_SORT_ORDER = SortOrder.DESC.name();
    private static final String FROM_DEFAULT_POSITION = "0";
    public static final String PATH = "path";
    public static final String DOMAIN_NAME = "domainName";
    public static final String HTTPS = "https";

    /**
     * Get searchTerm from request query parameters.
     *
     * @param requestInfo requestInfo
     * @return searchTerm given in query parameter
     */
    public static String getSearchTerm(RequestInfo requestInfo) {
        return requestInfo.getQueryParameters().getOrDefault(SEARCH_TERM_KEY, SEARCH_ALL_PUBLICATIONS_DEFAULT_QUERY);
    }

    public static int getResults(RequestInfo requestInfo) {
        return Integer.parseInt(requestInfo.getQueryParameters().getOrDefault(RESULTS_KEY, RESULTS_DEFAULT_SIZE));
    }

    public static int getFrom(RequestInfo requestInfo) {
        return Integer.parseInt(requestInfo.getQueryParameters().getOrDefault(FROM_KEY, FROM_DEFAULT_POSITION));
    }

    public static String getOrderBy(RequestInfo requestInfo) {
        return requestInfo.getQueryParameters().getOrDefault(ORDERBY_KEY, ORDERBY_DEFAULT_POSITION);
    }

    public static SortOrder getSortOrder(RequestInfo requestInfo) {
        return SortOrder.fromString(requestInfo.getQueryParameters().getOrDefault(SORTORDER_KEY, DEFAULT_SORT_ORDER));
    }

    public static URI getRequestUri(RequestInfo requestInfo) {
        String path = getRequestPath(requestInfo);
        String domainName = getRequestDomainName(requestInfo);
        return new UriWrapper(HTTPS, domainName).addChild(path).getUri();
    }

    public static String getRequestPath(RequestInfo requestInfo) {
        return attempt(() -> requestInfo.getRequestContext()
                .get(PATH).asText())
                .orElseThrow();
    }

    public static String getRequestDomainName(RequestInfo requestInfo) {
        return attempt(() -> requestInfo.getRequestContext()
                .get(DOMAIN_NAME).asText())
                .orElseThrow();
    }

    public static Query toQuery(RequestInfo requestInfo) {
        return new Query(
                getSearchTerm(requestInfo),
                getResults(requestInfo),
                getFrom(requestInfo),
                getOrderBy(requestInfo),
                getSortOrder(requestInfo),
                getRequestUri(requestInfo)
        );
    }

}
