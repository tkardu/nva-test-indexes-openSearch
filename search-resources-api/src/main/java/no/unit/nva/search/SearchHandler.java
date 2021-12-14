package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import no.unit.nva.search.models.Query;
import no.unit.nva.search.restclients.IdentityClient;
import no.unit.nva.search.restclients.IdentityClientImpl;
import no.unit.nva.search.restclients.responses.UserResponse;
import nva.commons.apigateway.ApiGatewayHandler;
import nva.commons.apigateway.RequestInfo;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import org.elasticsearch.action.search.SearchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Set;

import static java.net.HttpURLConnection.HTTP_OK;
import static no.unit.nva.search.RequestUtil.toQuery;
import static no.unit.nva.search.SearchClientConfig.defaultSearchClient;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static nva.commons.core.attempt.Try.attempt;

public class SearchHandler extends ApiGatewayHandler<Void, String> {

    public static final String INDEX = "index";
    private final SearchClient searchClient;
    private final IdentityClient identityClient;

    private final Logger logger = LoggerFactory.getLogger(SearchHandler.class);

    @JacocoGenerated
    public SearchHandler() {
        this(new Environment(), defaultSearchClient(), defaultIdentityClient());
    }

    @JacocoGenerated
    private static IdentityClient defaultIdentityClient() {
        return new IdentityClientImpl();
    }

    public SearchHandler(Environment environment, SearchClient searchClient, IdentityClient identityClient) {
        super(Void.class, environment, objectMapperWithEmpty);
        this.searchClient = searchClient;
        this.identityClient = identityClient;
    }

    @Override
    protected String processInput(Void input, RequestInfo requestInfo, Context context) throws ApiGatewayException {

        String indexName = getIndexName(requestInfo);
        Set<URI> includedUnits = getIncludedUnitsForUser(requestInfo);
        logger.info("Included units for user: " + includedUnits);

        Query query = toQuery(requestInfo);

        SearchResponse searchResponse = searchClient.doSearch(query, indexName);
        return searchResponse.toString();
    }

    private Set<URI> getIncludedUnitsForUser(RequestInfo requestInfo) {
        String username = requestInfo.getFeideId().orElseThrow();
        UserResponse userResponse = identityClient.getUser(username).orElseThrow();
        return userResponse.getViewingScope().getIncludedUnits();
    }

    private String getIndexName(RequestInfo requestInfo) {
        String indexName = attempt(() -> requestInfo.getPathParameter(INDEX)).orElseThrow();
        return indexName;
    }

    @Override
    protected Integer getSuccessStatusCode(Void input, String output) {
        return HTTP_OK;
    }
}
