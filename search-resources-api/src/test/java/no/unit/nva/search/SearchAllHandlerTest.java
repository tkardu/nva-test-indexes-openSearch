package no.unit.nva.search;

import static java.net.HttpURLConnection.HTTP_OK;
import static no.unit.nva.search.RequestUtil.DOMAIN_NAME;
import static no.unit.nva.search.RequestUtil.PATH;
import static no.unit.nva.search.SearchAllHandler.PAGE_SIZE_QUERY_PARAM;
import static no.unit.nva.search.SearchHandler.DEFAULT_PAGE_SIZE;
import static no.unit.nva.search.SearchHandler.EXPECTED_ACCESS_RIGHT_FOR_VIEWING_MESSAGES_AND_DOI_REQUESTS;
import static no.unit.nva.search.SearchHandler.VIEWING_SCOPE_QUERY_PARAMETER;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static no.unit.nva.testutils.RandomDataGenerator.randomInteger;
import static no.unit.nva.testutils.RandomDataGenerator.randomString;
import static no.unit.nva.testutils.RandomDataGenerator.randomUri;
import static nva.commons.core.ioutils.IoUtils.stringFromResources;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNot.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import no.unit.nva.commons.json.JsonUtils;
import no.unit.nva.indexing.testutils.SearchResponseUtil;
import no.unit.nva.search.models.SearchResourcesResponse;
import no.unit.nva.search.restclients.IdentityClient;
import no.unit.nva.search.restclients.responses.UserResponse;
import no.unit.nva.search.restclients.responses.ViewingScope;
import no.unit.nva.testutils.HandlerRequestBuilder;
import nva.commons.apigateway.GatewayResponse;
import nva.commons.core.Environment;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.zalando.problem.Problem;

class SearchAllHandlerTest {

    public static final String SAMPLE_ELASTICSEARCH_RESPONSE_JSON = "sample_elasticsearch_response.json";
    public static final String RESOURCE_ID = "f367b260-c15e-4d0f-b197-e1dc0e9eb0e8";
    public static final URI CUSTOMER_CRISTIN_ID = URI.create("https://example.org/123.XXX.XXX.XXX");
    public static final URI SOME_LEGAL_CUSTOM_CRISTIN_ID = URI.create("https://example.org/123.111.222.333");
    public static final URI SOME_ILLEGAL_CUSTOM_CRISTIN_ID = URI.create("https://example.org/124.111.222.333");

    public static final String SAMPLE_DOMAIN_NAME = "localhost";
    private static final String WORKLIST_PATH = "worklist";
    private static final String USERNAME = randomString();

    private IdentityClient identityClientMock;
    private SearchAllHandler handler;
    private Context context;
    private ByteArrayOutputStream outputStream;
    private FakeRestHighLevelClientWrapper restHighLevelClientWrapper;
    private SearchClient searchClient;

    @BeforeEach
    void init() throws IOException {
        prepareSearchClientWithResponse();
        setupFakeIdentityClient();
        searchClient = new SearchClient(restHighLevelClientWrapper);
        handler = initializeHandler();
        context = mock(Context.class);
        outputStream = new ByteArrayOutputStream();
    }

    @Test
    void shouldSentRequestWithGivenPageSize() throws IOException {

        var expectedPageSize = randomInteger();
        var request = createRequestWithPageSize(expectedPageSize);
        handler.handleRequest(request, outputStream, context);
        var response = GatewayResponse.fromOutputStream(outputStream, SearchResourcesResponse.class);
        assertThat(response.getStatusCode(), is(equalTo(HTTP_OK)));
        var actualPageSize = restHighLevelClientWrapper.getSearchRequest().source().size();
        assertThat(actualPageSize, equalTo(expectedPageSize));
    }

    @Test
    void shouldSentDefaultPageSizeRequestWhenPageSizeNotSubmitted() throws IOException {

        var request = queryWithoutQueryParameters();
        handler.handleRequest(request, outputStream, context);
        var response = GatewayResponse.fromOutputStream(outputStream, SearchResourcesResponse.class);
        assertThat(response.getStatusCode(), is(equalTo(HTTP_OK)));
        var actualPageSize = restHighLevelClientWrapper.getSearchRequest().source().size();
        assertThat(actualPageSize, equalTo(DEFAULT_PAGE_SIZE));
    }

    @Test
    void shouldReturnSearchResponseWithSearchHit() throws IOException {
        handler.handleRequest(queryWithoutQueryParameters(), outputStream, context);

        GatewayResponse<String> response = GatewayResponse.fromOutputStream(outputStream, String.class);

        assertThat(response.getStatusCode(), is(equalTo(HTTP_OK)));
        assertThat(response.getBody(), containsString(RESOURCE_ID));

        JsonNode jsonNode = objectMapperWithEmpty.readTree(response.getBody());
        assertThat(jsonNode, is(notNullValue()));
    }

    @Test
    void shouldSendQueryOverridingDefaultViewingScopeWhenUserRequestsToViewDoiRequestsOrMessagesWithinTheirLegalScope()
        throws IOException {
        handler.handleRequest(queryWithCustomOrganizationAsQueryParameter(SOME_LEGAL_CUSTOM_CRISTIN_ID),
                              outputStream,
                              context);
        var searchRequest = restHighLevelClientWrapper.getSearchRequest();
        var queryDescription = searchRequest.buildDescription();

        assertThat(queryDescription, containsString(SOME_LEGAL_CUSTOM_CRISTIN_ID.toString()));
        assertThatDefaultScopeHasBeenOverridden(queryDescription);
    }

    @Test
    void shouldNotSendQueryAndReturnForbiddenWhenUserRequestsToViewDoiRequestsOrMessagesOutsideTheirLegalScope()
        throws IOException {
        handler.handleRequest(queryWithCustomOrganizationAsQueryParameter(SOME_ILLEGAL_CUSTOM_CRISTIN_ID),
                              outputStream,
                              context);
        GatewayResponse<Problem> response = GatewayResponse.fromOutputStream(outputStream, Problem.class);
        assertThat(response.getStatusCode(), is(equalTo(HttpURLConnection.HTTP_FORBIDDEN)));

        var searchRequest = restHighLevelClientWrapper.getSearchRequest();
        assertThat(searchRequest, is(nullValue()));
    }

    @Test
    void shouldSendQueryWhenDefaultScopeIsNotOverriddenByUser() throws IOException {
        handler.handleRequest(queryWithoutQueryParameters(), outputStream, context);
        var searchRequest = restHighLevelClientWrapper.getSearchRequest();
        var queryDescription = searchRequest.buildDescription();

        for (var uriInDefaultViewingScope : includedUrisInDefaultViewingScope()) {
            assertThat(queryDescription, containsString(uriInDefaultViewingScope.toString()));
        }
    }

    @Test
    void shouldNotSendQueryAndReturnForbiddenWhenUserDoesNotHaveTheAppropriateAccessRigth() throws IOException {
        handler.handleRequest(queryWithoutAppropriateAccessRight(), outputStream, context);
        GatewayResponse<Problem> response = GatewayResponse.fromOutputStream(outputStream, Problem.class);
        assertThat(response.getStatusCode(), is(equalTo(HttpURLConnection.HTTP_FORBIDDEN)));
        var searchRequest = restHighLevelClientWrapper.getSearchRequest();
        assertThat(searchRequest, is(nullValue()));
    }

    @Test
    void shouldSendQueryIncludingAllIndicesRelevantToCuratorsWorklist() throws IOException {
        handler.handleRequest(queryWithoutQueryParameters(), outputStream, context);
        var searchRequest = restHighLevelClientWrapper.getSearchRequest();
        var indices = Arrays.stream(searchRequest.indices()).collect(Collectors.toList());
        assertThat(indices, containsInAnyOrder("messages", "doirequests"));
    }

    private InputStream createRequestWithPageSize(Integer expectedPageSize) throws JsonProcessingException {
        return new HandlerRequestBuilder<>(JsonUtils.dtoObjectMapper)
            .withQueryParameters(Map.of(PAGE_SIZE_QUERY_PARAM, expectedPageSize.toString()))
            .withNvaUsername(USERNAME)
            .withAccessRight(EXPECTED_ACCESS_RIGHT_FOR_VIEWING_MESSAGES_AND_DOI_REQUESTS)
            .withRequestContextValue(PATH, WORKLIST_PATH)
            .withRequestContextValue(DOMAIN_NAME, SAMPLE_DOMAIN_NAME).build();
    }

    @Test
    @DisplayName("should send query with viewing scope equal to TopCristinOrgId when no custom"
                 + "viewing scope has been added to the query or user profile does not contain a"
                 + "viewing scope")
    void shouldSendQueryWithViewingScopeEqualToUserTopCristingOrgIdWhenNoOtherViewingScopeIsSet()
        throws IOException {
        fakeIdentityClientReturnsUserWithoutViewingScope();
        handler = initializeHandler();
        handler.handleRequest(queryWithoutQueryParameters(), outputStream, context);

        var searchRequest = restHighLevelClientWrapper.getSearchRequest();
        var query = ((BoolQueryBuilder) searchRequest.source().query());
        var actualViewingScope = query.must().stream()
            .map(Object::toString)
            .filter(clause -> containsOneOfExpectedStrings(clause, List.of(CUSTOMER_CRISTIN_ID.toString())))
            .collect(Collectors.toList());

        assertThat(actualViewingScope.size(), is(equalTo(1)));
    }

    private SearchAllHandler initializeHandler() {
        return new SearchAllHandler(new Environment(), searchClient, identityClientMock);
    }

    private boolean containsOneOfExpectedStrings(String clause, List<String> expectedViewingScopeUris) {
        return expectedViewingScopeUris.stream().anyMatch(clause::contains);
    }

    private void assertThatDefaultScopeHasBeenOverridden(String queryDescription) {
        var notExpectedDefaultViewingUris = includedUrisInDefaultViewingScope();
        for (var notExpectedUri : notExpectedDefaultViewingUris) {
            assertThat(queryDescription, not(containsString(notExpectedUri.toString())));
        }
    }

    private Set<URI> includedUrisInDefaultViewingScope() {
        return identityClientMock.getUser(USERNAME)
            .map(UserResponse::getViewingScope)
            .map(ViewingScope::getIncludedUnits)
            .orElseThrow();
    }

    private void setupFakeIdentityClient() {
        identityClientMock = mock(IdentityClient.class);
        when(identityClientMock.getUser(anyString())).thenReturn(getUserResponse());
    }

    private void fakeIdentityClientReturnsUserWithoutViewingScope() {
        identityClientMock = mock(IdentityClient.class);
        when(identityClientMock.getUser(anyString())).thenReturn(userWithoutViewingScope());
    }

    private Optional<UserResponse> userWithoutViewingScope() {
        return Optional.of(new UserResponse());
    }

    private Optional<UserResponse> getUserResponse() {
        UserResponse userResponse = new UserResponse();
        ViewingScope viewingScope = new ViewingScope();
        viewingScope.setIncludedUnits(Set.of(randomUri(), randomUri()));
        viewingScope.setExcludedUnits(Collections.emptySet());
        userResponse.setViewingScope(viewingScope);
        return Optional.of(userResponse);
    }

    private void prepareSearchClientWithResponse() throws IOException {
        RestHighLevelClient restHighLevelClientMock = mock(RestHighLevelClient.class);
        when(restHighLevelClientMock.search(any(), any())).thenReturn(getSearchResponse());
        restHighLevelClientWrapper = new FakeRestHighLevelClientWrapper(restHighLevelClientMock);
    }

    private InputStream queryWithoutQueryParameters() throws JsonProcessingException {
        return new HandlerRequestBuilder<Void>(objectMapperWithEmpty)
            .withNvaUsername(USERNAME)
            .withTopLevelCristinOrgId(CUSTOMER_CRISTIN_ID)
            .withAccessRight(EXPECTED_ACCESS_RIGHT_FOR_VIEWING_MESSAGES_AND_DOI_REQUESTS)
            .withRequestContextValue(PATH, WORKLIST_PATH)
            .withRequestContextValue(DOMAIN_NAME, SAMPLE_DOMAIN_NAME)
            .build();
    }

    private InputStream queryWithCustomOrganizationAsQueryParameter(URI desiredOrgUri) throws JsonProcessingException {
        return new HandlerRequestBuilder<Void>(objectMapperWithEmpty)
            .withQueryParameters(Map.of(VIEWING_SCOPE_QUERY_PARAMETER, desiredOrgUri.toString()))
            .withNvaUsername(USERNAME)
            .withAccessRight(EXPECTED_ACCESS_RIGHT_FOR_VIEWING_MESSAGES_AND_DOI_REQUESTS)
            .withTopLevelCristinOrgId(CUSTOMER_CRISTIN_ID)
            .withRequestContextValue(PATH, WORKLIST_PATH)
            .withRequestContextValue(DOMAIN_NAME, SAMPLE_DOMAIN_NAME)
            .build();
    }

    private InputStream queryWithoutAppropriateAccessRight() throws JsonProcessingException {
        return new HandlerRequestBuilder<Void>(objectMapperWithEmpty)
            .withNvaUsername(USERNAME)
            .withAccessRight("SomeOtherAccessRight")
            .withTopLevelCristinOrgId(CUSTOMER_CRISTIN_ID)
            .withRequestContextValue(PATH, WORKLIST_PATH)
            .withRequestContextValue(DOMAIN_NAME, SAMPLE_DOMAIN_NAME)
            .build();
    }

    private SearchResponse getSearchResponse() throws IOException {
        String jsonResponse = stringFromResources(Path.of(SAMPLE_ELASTICSEARCH_RESPONSE_JSON));
        return SearchResponseUtil.getSearchResponseFromJson(jsonResponse);
    }
}
