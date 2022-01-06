package no.unit.nva.search;

import static java.net.HttpURLConnection.HTTP_OK;
import static no.unit.nva.search.RequestUtil.DOMAIN_NAME;
import static no.unit.nva.search.RequestUtil.PATH;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static no.unit.nva.testutils.RandomDataGenerator.randomUri;
import static nva.commons.core.ioutils.IoUtils.stringFromResources;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import no.unit.nva.indexing.testutils.SearchResponseUtil;
import no.unit.nva.search.restclients.IdentityClient;
import no.unit.nva.search.restclients.responses.UserResponse;
import no.unit.nva.search.restclients.responses.UserResponse.ViewingScope;
import no.unit.nva.testutils.HandlerRequestBuilder;
import nva.commons.apigateway.GatewayResponse;
import nva.commons.core.Environment;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SearchHandlerTest {

    public static final String SAMPLE_ELASTICSEARCH_RESPONSE_JSON = "sample_elasticsearch_response.json";
    public static final String RESOURCE_ID = "bd0f0ba3-e17d-473c-b6d5-d97447b26332";
    public static final String MESSAGE = "message";
    public static final String INDEX = "index";
    public static final String SAMPLE_FEIDE_ID = "user@localhost";
    public static final String VIEWING_SCOPE_QUERY_PARAMETER = "viewingScope";

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
    void shouldSendQueryOverridingDefaultViewingScopeWhenAnOrganizationIsSpecifiedInQueryParameter()
        throws IOException {
        final URI desiredOrgUri = randomUri();
        handler.handleRequest(queryWithCustomOrganizationAsQueryParameter(desiredOrgUri), outputStream, context);
        SearchRequest searchRequest = restHighLevelClientWrapper.getSearchRequest();
        String queryDescription=searchRequest.buildDescription();

        final var notExpectedDefaultViewingUris = identityClientMock.getUser(SAMPLE_FEIDE_ID)
            .map(UserResponse::getViewingScope)
            .map(ViewingScope::getIncludedUnits)
            .orElseThrow();

        assertThat(queryDescription, containsString(desiredOrgUri.toString()));

        for(URI notExpectedUri:notExpectedDefaultViewingUris){
            assertThat(queryDescription,not(containsString(notExpectedUri.toString())));
        }
    }

    private void setupFakeIdentityClient() {
        identityClientMock = mock(IdentityClient.class);
        when(identityClientMock.getUser(anyString())).thenReturn(getUserResponse());
    }

    private Optional<UserResponse> getUserResponse() {
        UserResponse userResponse = new UserResponse();
        UserResponse.ViewingScope viewingScope = new UserResponse.ViewingScope();
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
            .withPathParameters(Map.of(INDEX, MESSAGE))
            .withFeideId(SAMPLE_FEIDE_ID)
            .build();
    }

    private InputStream queryWithCustomOrganizationAsQueryParameter(URI desiredOrgUri) throws JsonProcessingException {
        return new HandlerRequestBuilder<Void>(objectMapperWithEmpty)
            .withPathParameters(Map.of(INDEX, MESSAGE))
            .withQueryParameters(Map.of(VIEWING_SCOPE_QUERY_PARAMETER, desiredOrgUri.toString()))
            .withFeideId(SAMPLE_FEIDE_ID)
            .build();
    }


    private SearchResponse getSearchResponse() throws IOException {
        String jsonResponse = stringFromResources(Path.of(SAMPLE_ELASTICSEARCH_RESPONSE_JSON));
        return SearchResponseUtil.getSearchResponseFromJson(jsonResponse);
    }
}
