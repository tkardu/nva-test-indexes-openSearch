package no.unit.nva.search;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.nonNull;
import static java.util.function.Predicate.isEqual;
import static no.unit.nva.search.SearchClientConfig.defaultSearchClient;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static nva.commons.core.attempt.Try.attempt;
import com.amazonaws.services.lambda.runtime.Context;
import java.net.URI;
import java.util.Collection;
import java.util.Optional;
import no.unit.nva.search.models.SearchResourcesResponse;
import no.unit.nva.search.restclients.IdentityClient;
import no.unit.nva.search.restclients.IdentityClientImpl;
import no.unit.nva.search.restclients.responses.UserResponse;
import no.unit.nva.search.restclients.responses.ViewingScope;
import nva.commons.apigateway.ApiGatewayHandler;
import nva.commons.apigateway.RequestInfo;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.apigateway.exceptions.ForbiddenException;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.attempt.Try;
import nva.commons.core.paths.UriWrapper;
import org.elasticsearch.action.search.SearchResponse;

public class SearchAllHandler extends ApiGatewayHandler<Void, SearchResourcesResponse> {

    public static final String VIEWING_SCOPE_QUERY_PARAMETER = "viewingScope";
    public static final String CRISTIN_ORG_LEVEL_DELIMITER = "\\.";
    public static final int HIGHEST_LEVEL_ORGANIZATION = 0;
    public static final String EXPECTED_ACCESS_RIGHT_FOR_VIEWING_MESSAGES_AND_DOI_REQUESTS = "APPROVE_DOI_REQUEST";
    public static final String PAGE_SIZE_QUERY_PARAM = "pageSize";
    private static final String[] CURATOR_WORKLIST_INDICES = {"messages", "doirequests"};
    private static final int DEFAULT_PAGE_SIZE = 100;
    private final SearchClient searchClient;
    private final IdentityClient identityClient;

    @JacocoGenerated
    public SearchAllHandler() {
        this(new Environment(), defaultSearchClient(), defaultIdentityClient());
    }

    public SearchAllHandler(Environment environment, SearchClient searchClient, IdentityClient identityClient) {
        super(Void.class, environment, objectMapperWithEmpty);
        this.searchClient = searchClient;
        this.identityClient = identityClient;
    }

    @Override
    protected SearchResourcesResponse processInput(Void input, RequestInfo requestInfo, Context context)
        throws ApiGatewayException {
        assertUserHasAppropriateAccessRights(requestInfo);
        ViewingScope viewingScope = getViewingScopeForUser(requestInfo);
        SearchResponse searchResponse = searchClient.findResourcesForOrganizationIds(viewingScope,
                                                                                     extractPageSize(requestInfo),
                                                                                     CURATOR_WORKLIST_INDICES);

        URI requestUri = RequestUtil.getRequestUri(requestInfo);
        return SearchResourcesResponse.fromSearchResponse(searchResponse, requestUri);
    }

    @Override
    protected Integer getSuccessStatusCode(Void input, SearchResourcesResponse output) {
        return HTTP_OK;
    }

    @JacocoGenerated
    private static IdentityClient defaultIdentityClient() {
        return new IdentityClientImpl();
    }

    private Integer extractPageSize(RequestInfo requestInfo) {
        return requestInfo.getQueryParameterOpt(PAGE_SIZE_QUERY_PARAM)
            .map(Integer::valueOf)
            .orElse(DEFAULT_PAGE_SIZE);
    }

    private void assertUserHasAppropriateAccessRights(RequestInfo requestInfo) throws ForbiddenException {
        if (!requestInfo.userIsAuthorized(EXPECTED_ACCESS_RIGHT_FOR_VIEWING_MESSAGES_AND_DOI_REQUESTS)) {
            throw new ForbiddenException();
        }
    }

    private ViewingScope getViewingScopeForUser(RequestInfo requestInfo) throws ApiGatewayException {
        var userDefinedScope = getUserDefinedViewingScope(requestInfo);
        return userDefinedScope
            .orElseGet(() -> defaultViewingScope(requestInfo))
            .orElseThrow(failure -> handleFailure(failure.getException()));
    }

    private ApiGatewayException handleFailure(Exception exception) {
        if (exception instanceof ForbiddenException) {
            return (ForbiddenException) exception;
        }
        throw new RuntimeException(exception);
    }

    private Try<ViewingScope> defaultViewingScope(RequestInfo requestInfo) {
        var defaultViewingScope = fetchViewingScopeFromUserProfile(requestInfo)
            .orElseGet(() -> createDefaultViewingScopeBasedOnUserLoginData(requestInfo));
        return Try.of(defaultViewingScope);
    }

    private Optional<ViewingScope> fetchViewingScopeFromUserProfile(RequestInfo requestInfo) {
        return attempt(requestInfo::getNvaUsername)
            .map(nvaUsername -> identityClient.getUser(nvaUsername, requestInfo.getAuthHeader()))
            .map(Optional::orElseThrow)
            .map(UserResponse::getViewingScope)
            .toOptional()
            .filter(viewingScope -> isNotEmpty(viewingScope.getIncludedUnits()));
    }

    private <T> boolean isNotEmpty(Collection<T> collection) {
        return nonNull(collection) && !collection.isEmpty();
    }

    private ViewingScope createDefaultViewingScopeBasedOnUserLoginData(RequestInfo requestInfo) {
        return requestInfo.getTopLevelOrgCristinId().map(ViewingScope::create).orElseThrow();
    }

    private Optional<Try<ViewingScope>> getUserDefinedViewingScope(RequestInfo requestInfo) {
        return requestInfo.getQueryParameterOpt(VIEWING_SCOPE_QUERY_PARAMETER)
            .map(URI::create)
            .map(ViewingScope::create)
            .map(attempt(scope -> authorizeCustomViewingScope(scope, requestInfo)));
    }

    private ViewingScope authorizeCustomViewingScope(ViewingScope viewingScope, RequestInfo requestInfo)
        throws ForbiddenException {
        var customerCristinId = requestInfo.getTopLevelOrgCristinId().orElseThrow();
        return userIsAuthorized(viewingScope, customerCristinId);
    }

    private ViewingScope userIsAuthorized(ViewingScope viewingScope, URI customerCristinId) throws ForbiddenException {
        if (allIncludedUnitsAreLegal(viewingScope, customerCristinId)) {
            return viewingScope;
        }
        throw new ForbiddenException();
    }

    private boolean allIncludedUnitsAreLegal(ViewingScope viewingScope, URI customerCristinId) {
        return viewingScope.getIncludedUnits().stream()
            .map(requestedOrg -> isUnderUsersInstitution(requestedOrg, customerCristinId))
            .allMatch(isEqual(true));
    }

    private boolean isUnderUsersInstitution(URI requestedOrg, URI customerCristinId) {
        String requestedOrgInstitutionNumber = extractInstitutionNumberFromRequestedOrganization(requestedOrg);
        String customerCristinInstitutionNumber = extractInstitutionNumberFromRequestedOrganization(customerCristinId);
        return customerCristinInstitutionNumber.equals(requestedOrgInstitutionNumber);
    }

    private String extractInstitutionNumberFromRequestedOrganization(URI requestedOrg) {
        String requestedOrgCristinIdentifier = UriWrapper.fromUri(requestedOrg).getLastPathElement();
        return requestedOrgCristinIdentifier.split(CRISTIN_ORG_LEVEL_DELIMITER)[HIGHEST_LEVEL_ORGANIZATION];
    }
}
