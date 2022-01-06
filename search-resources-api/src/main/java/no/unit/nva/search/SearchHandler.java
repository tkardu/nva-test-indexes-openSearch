package no.unit.nva.search;

import static java.net.HttpURLConnection.HTTP_OK;
import static no.unit.nva.search.SearchClientConfig.defaultSearchClient;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static nva.commons.core.attempt.Try.attempt;
import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.util.Optional;
import no.unit.nva.search.restclients.IdentityClient;
import no.unit.nva.search.restclients.IdentityClientImpl;
import no.unit.nva.search.restclients.responses.UserResponse;
import no.unit.nva.search.restclients.responses.UserResponse.ViewingScope;
import nva.commons.apigateway.ApiGatewayHandler;
import nva.commons.apigateway.RequestInfo;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import org.elasticsearch.action.search.SearchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchHandler extends ApiGatewayHandler<Void, JsonNode> {

    public static final String INDEX = "index";
    public static final String VIEWING_SCOPE_QUERY_PARAMETER = "viewingScope";
    private final SearchClient searchClient;
    private final IdentityClient identityClient;

    @JacocoGenerated
    public SearchHandler() {
        this(new Environment(), defaultSearchClient(), defaultIdentityClient());
    }

    public SearchHandler(Environment environment, SearchClient searchClient, IdentityClient identityClient) {
        super(Void.class, environment, objectMapperWithEmpty);
        this.searchClient = searchClient;
        this.identityClient = identityClient;
    }

    @Override
    protected JsonNode processInput(Void input, RequestInfo requestInfo, Context context) throws ApiGatewayException {

        String indexName = getIndexName(requestInfo);
        UserResponse.ViewingScope viewingScope = getViewingScopeForUser(requestInfo);
        SearchResponse searchResponse = searchClient.findResourcesForOrganizationIds(indexName, viewingScope);
        return toJsonNode(searchResponse);
    }

    @Override
    protected Integer getSuccessStatusCode(Void input, JsonNode output) {
        return HTTP_OK;
    }

    @JacocoGenerated
    private static IdentityClient defaultIdentityClient() {
        return new IdentityClientImpl();
    }

    private JsonNode toJsonNode(SearchResponse searchResponse) {
        return attempt(() -> objectMapperWithEmpty.readTree(searchResponse.toString())).orElseThrow();
    }

    private UserResponse.ViewingScope getViewingScopeForUser(RequestInfo requestInfo) {
        return getUserDefinedViewingScore(requestInfo).orElseGet(() -> defaultViewingScope(requestInfo));
    }

    private ViewingScope defaultViewingScope(RequestInfo requestInfo) {
        String username = requestInfo.getFeideId().orElseThrow();
        UserResponse userResponse = identityClient.getUser(username).orElseThrow();
        return userResponse.getViewingScope();
    }

    private Optional<ViewingScope> getUserDefinedViewingScore(RequestInfo requestInfo) {
        return attempt(() -> requestInfo.getQueryParameter(VIEWING_SCOPE_QUERY_PARAMETER))
            .map(URI::create)
            .map(ViewingScope::create)
            .toOptional();
    }

    private String getIndexName(RequestInfo requestInfo) {
        String indexName = attempt(() -> requestInfo.getPathParameter(INDEX)).orElseThrow();
        return indexName;
    }
}
