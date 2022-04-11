package no.unit.nva.search.restclients;

import static no.unit.nva.testutils.RandomDataGenerator.randomString;
import static no.unit.nva.testutils.RandomDataGenerator.randomUri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import no.unit.nva.search.restclients.responses.UserResponse;
import no.unit.nva.search.restclients.responses.ViewingScope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IdentityClientImplTest {

    public static final String SAMPLE_USERNAME = "user@localhost";
    private IdentityClient identityClient;
    private HttpClient httpClientMock;

    @BeforeEach
    void init()  {
        httpClientMock = mock(HttpClient.class);
        identityClient = new IdentityClientImpl(httpClientMock);
    }

    @Test
    void shouldReturnUserResponseWithTwoIncludedUnits() throws IOException, InterruptedException {
        UserResponse userResponse = getUserResponse();
        prepareOkResponse(userResponse);

        Optional<UserResponse> user = identityClient.getUser(SAMPLE_USERNAME, randomString());
        assertThat(user.isPresent(), is(equalTo(true)));
        var actualIncludedUnits = user.get().getViewingScope().getIncludedUnits();
        var expectedIncludedUnits = userResponse.getViewingScope().getIncludedUnits();
        assertThat(actualIncludedUnits, is(equalTo(expectedIncludedUnits)));
    }

    @Test
    void shouldReturnOptionalEmptyWhenUserNotFound() throws IOException, InterruptedException {
        prepareNotFoundResponse();

        Optional<UserResponse> user = identityClient.getUser(SAMPLE_USERNAME, randomString());
        assertThat(user.isPresent(), is(equalTo(false)));
    }

    private void prepareNotFoundResponse() throws IOException, InterruptedException {
        HttpResponse<String> httpResponse = mock(HttpResponse.class);
        when(httpResponse.statusCode()).thenReturn(HttpURLConnection.HTTP_NOT_FOUND);
        when(httpClientMock.<String>send(any(), any())).thenReturn(httpResponse);
    }

    private void prepareOkResponse(UserResponse userResponse) throws IOException, InterruptedException {
        HttpResponse<String> httpResponse = mock(HttpResponse.class);
        when(httpResponse.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
        when(httpResponse.body()).thenReturn(userResponse.toJson());
        when(httpClientMock.<String>send(any(), any())).thenReturn(httpResponse);
    }

    private UserResponse getUserResponse() {
        var userResponse = new UserResponse();
        var viewingScope = new ViewingScope();
        viewingScope.setIncludedUnits(Set.of(randomUri(), randomUri()));
        viewingScope.setExcludedUnits(Collections.emptySet());
        userResponse.setViewingScope(viewingScope);
        return userResponse;
    }
}