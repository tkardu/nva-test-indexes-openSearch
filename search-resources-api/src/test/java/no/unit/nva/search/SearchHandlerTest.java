package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import no.unit.nva.indexing.testutils.SearchResponseUtil;
import no.unit.nva.search.restclients.IdentityClient;
import no.unit.nva.search.restclients.responses.UserResponse;
import no.unit.nva.search.restclients.responses.ViewingScope;
import no.unit.nva.testutils.HandlerRequestBuilder;
import nva.commons.apigateway.GatewayResponse;
import nva.commons.core.Environment;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.zalando.problem.Problem;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.net.HttpURLConnection.HTTP_OK;
import static no.unit.nva.search.RequestUtil.DOMAIN_NAME;
import static no.unit.nva.search.RequestUtil.PATH;
import static no.unit.nva.search.SearchHandler.EXPECTED_ACCESS_RIGHT_FOR_VIEWING_MESSAGES_AND_DOI_REQUESTS;
import static no.unit.nva.search.SearchHandler.VIEWING_SCOPE_QUERY_PARAMETER;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static no.unit.nva.testutils.RandomDataGenerator.randomUri;
import static nva.commons.core.ioutils.IoUtils.stringFromResources;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
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

public class SearchHandlerTest {

    public static final String SAMPLE_ELASTICSEARCH_RESPONSE_JSON = "sample_elasticsearch_response.json";
    public static final String RESOURCE_ID = "f367b260-c15e-4d0f-b197-e1dc0e9eb0e8";
    public static final String SAMPLE_FEIDE_ID = "user@localhost";
    public static final URI CUSTOMER_CRISTIN_ID = URI.create("https://example.org/123.XXX.XXX.XXX");
    public static final URI SOME_LEGAL_CUSTOM_CRISTIN_ID = URI.create("https://example.org/123.111.222.333");
    public static final URI SOME_ILLEGAL_CUSTOM_CRISTIN_ID = URI.create("https://example.org/124.111.222.333");
    public static final String MESSAGES_PATH = "/messages";
    public static final String SAMPLE_PATH = "search";
    public static final String SAMPLE_DOMAIN_NAME = "localhost";

    private IdentityClient identityClientMock;
    private SearchHandler handler;
    private Context context;
    private ByteArrayOutputStream outputStream;
    private FakeRestHighLevelClientWrapper restHighLevelClientWrapper;

    @BeforeEach
    void init() throws IOException {
        prepareSearchClientWithResponse();
        SearchClient searchClient = new SearchClient(restHighLevelClientWrapper);
        setupFakeIdentityClient();
        handler = new SearchHandler(new Environment(), searchClient, identityClientMock);
        context = mock(Context.class);
        outputStream = new ByteArrayOutputStream();
    }

    @Test
    void shouldReturnSearchResponseWithSearchHit() throws IOException {
        handler.handleRequest(queryWithoutQueryParameters(), outputStream, context);

        GatewayResponse<String> response = GatewayResponse.fromOutputStream(outputStream);

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
        GatewayResponse<Problem> response = GatewayResponse.fromOutputStream(outputStream);
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
        GatewayResponse<Problem> response = GatewayResponse.fromOutputStream(outputStream);
        assertThat(response.getStatusCode(), is(equalTo(HttpURLConnection.HTTP_FORBIDDEN)));
        var searchRequest = restHighLevelClientWrapper.getSearchRequest();
        assertThat(searchRequest, is(nullValue()));
    }

    @ParameterizedTest(name = "should send request to index specified in path")
    @ValueSource(strings = {"/messages","/doirequests"})
    void shouldReturnIndexNameFromPath(String path) throws IOException {
        handler.handleRequest(queryWithoutQueryParameters(path), outputStream, context);
        var searchRequest = restHighLevelClientWrapper.getSearchRequest();
        var indices = Arrays.stream(searchRequest.indices()).collect(Collectors.toList());
        assertThat(indices, contains(path.substring(1)));
    }

    private void assertThatDefaultScopeHasBeenOverridden(String queryDescription) {
        var notExpectedDefaultViewingUris = includedUrisInDefaultViewingScope();
        for (var notExpectedUri : notExpectedDefaultViewingUris) {
            assertThat(queryDescription, not(containsString(notExpectedUri.toString())));
        }
    }

    private Set<URI> includedUrisInDefaultViewingScope() {
        return identityClientMock.getUser(SAMPLE_FEIDE_ID)
            .map(UserResponse::getViewingScope)
            .map(ViewingScope::getIncludedUnits)
            .orElseThrow();
    }

    private void setupFakeIdentityClient() {
        identityClientMock = mock(IdentityClient.class);
        when(identityClientMock.getUser(anyString())).thenReturn(getUserResponse());
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

    //TODO: add support for this in testutils module
    private void addPathAndDomainNameToRequestContext(HandlerRequestBuilder<Void> builder, String path) {
        var requestContext = builder.getRequestContext();
        requestContext.put(PATH, path);
        requestContext.put(DOMAIN_NAME, SAMPLE_DOMAIN_NAME);
        builder.withRequestContext(requestContext);
    }

    private InputStream queryWithoutQueryParameters(String path) throws JsonProcessingException {
        var builder = new HandlerRequestBuilder<Void>(objectMapperWithEmpty)
                .withFeideId(SAMPLE_FEIDE_ID)
                .withAccessRight(EXPECTED_ACCESS_RIGHT_FOR_VIEWING_MESSAGES_AND_DOI_REQUESTS);
        addPathAndDomainNameToRequestContext(builder, path);
        return builder.build();
    }

    private InputStream queryWithoutQueryParameters() throws JsonProcessingException {
        return queryWithoutQueryParameters(MESSAGES_PATH);
    }

    private InputStream queryWithCustomOrganizationAsQueryParameter(URI desiredOrgUri) throws JsonProcessingException {
        var builder = new HandlerRequestBuilder<Void>(objectMapperWithEmpty)
            .withQueryParameters(Map.of(VIEWING_SCOPE_QUERY_PARAMETER, desiredOrgUri.toString()))
            .withFeideId(SAMPLE_FEIDE_ID)
            .withAccessRight(EXPECTED_ACCESS_RIGHT_FOR_VIEWING_MESSAGES_AND_DOI_REQUESTS)
            .withCustomerCristinId(CUSTOMER_CRISTIN_ID.toString());
        addPathAndDomainNameToRequestContext(builder, MESSAGES_PATH);
        return builder.build();
    }

    private InputStream queryWithoutAppropriateAccessRight() throws JsonProcessingException {
        var builder = new HandlerRequestBuilder<Void>(objectMapperWithEmpty)
            .withFeideId(SAMPLE_FEIDE_ID)
            .withAccessRight("SomeOtherAccessRight")
            .withCustomerCristinId(CUSTOMER_CRISTIN_ID.toString());
        addPathAndDomainNameToRequestContext(builder, MESSAGES_PATH);
        return builder.build();
    }

    private SearchResponse getSearchResponse() throws IOException {
        String jsonResponse = stringFromResources(Path.of(SAMPLE_ELASTICSEARCH_RESPONSE_JSON));
        return SearchResponseUtil.getSearchResponseFromJson(jsonResponse);
    }

}
