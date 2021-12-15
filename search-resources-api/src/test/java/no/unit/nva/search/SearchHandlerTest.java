package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.unit.nva.indexing.testutils.SearchResponseUtil;
import no.unit.nva.search.restclients.IdentityClient;
import no.unit.nva.search.restclients.responses.UserResponse;
import no.unit.nva.testutils.HandlerRequestBuilder;
import nva.commons.apigateway.GatewayResponse;
import nva.commons.core.Environment;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchHandlerTest {

    public static final String SAMPLE_ELASTICSEARCH_RESPONSE_JSON = "sample_elasticsearch_response.json";
    public static final String RESOURCE_ID = "bd0f0ba3-e17d-473c-b6d5-d97447b26332";
    public static final String MESSAGE = "message";
    public static final String INDEX = "index";
    public static final String QUERY = "query";
    public static final String WILDCARD = "*";
    public static final String SAMPLE_PATH = "search";
    public static final String SAMPLE_DOMAIN_NAME = "localhost";
    public static final Map<String, Map<String, String>> SAMPLE_CLAIMS_WITH_FEIDE_ID = Map.of(
            "claims", Map.of("custom:feideId", "user@localhost"));
    public static final String AUTHORIZER = "authorizer";

    private IdentityClient identityClientMock;
    private RestHighLevelClient restHighLevelClientMock;
    private SearchHandler handler;
    private Context contextMock;
    private ByteArrayOutputStream outputStream;

    @BeforeEach
    void init() {
        restHighLevelClientMock = mock(RestHighLevelClient.class);
        RestHighLevelClientWrapper restHighLevelClientWrapper = new RestHighLevelClientWrapper(restHighLevelClientMock);
        SearchClient searchClient = new SearchClient(restHighLevelClientWrapper);
        identityClientMock = mock(IdentityClient.class);
        handler = new SearchHandler(new Environment(), searchClient, identityClientMock);
        contextMock = mock(Context.class);
        outputStream = new ByteArrayOutputStream();
    }

    @Test
    public void shouldReturnSearchResponseWithSearchHit() throws IOException {

        prepareSearchClientWithResponse();
        prepareIdentityClientWithResponse();

        handler.handleRequest(getInputStream(), outputStream, contextMock);

        GatewayResponse<String> response = GatewayResponse.fromOutputStream(outputStream);

        assertThat(response.getStatusCode(), is(equalTo(HTTP_OK)));
        assertThat(response.getBody(), containsString(RESOURCE_ID));

        JsonNode jsonNode = objectMapperWithEmpty.readTree(response.getBody());
        assertThat(jsonNode, is(notNullValue()));
    }

    private void prepareIdentityClientWithResponse() {
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
        when(restHighLevelClientMock.search(any(), any())).thenReturn(getSearchResponse());
    }

    private InputStream getInputStream() throws JsonProcessingException {
        return new HandlerRequestBuilder<Void>(objectMapperWithEmpty)
                .withPathParameters(Map.of(INDEX, MESSAGE))
                .withQueryParameters(Map.of(QUERY, WILDCARD))
                .withRequestContext(getRequestContext())
                .build();
    }

    private ObjectNode getRequestContext() {
        return objectMapperWithEmpty.convertValue(Map.of(
                PATH, SAMPLE_PATH, DOMAIN_NAME, SAMPLE_DOMAIN_NAME, AUTHORIZER, SAMPLE_CLAIMS_WITH_FEIDE_ID),
                ObjectNode.class);
    }

    private SearchResponse getSearchResponse() throws IOException {
        String jsonResponse = stringFromResources(Path.of(SAMPLE_ELASTICSEARCH_RESPONSE_JSON));
        return SearchResponseUtil.getSearchResponseFromJson(jsonResponse);
    }

}
