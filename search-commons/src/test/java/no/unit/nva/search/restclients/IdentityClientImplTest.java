package no.unit.nva.search.restclients;

import no.unit.nva.search.restclients.responses.UserResponse;
import nva.commons.logutils.LogUtils;
import nva.commons.logutils.TestAppender;
import nva.commons.secrets.ErrorReadingSecretException;
import nva.commons.secrets.SecretsReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static no.unit.nva.search.restclients.IdentityClientImpl.ERROR_READING_SECRETS_ERROR;
import static no.unit.nva.testutils.RandomDataGenerator.randomUri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IdentityClientImplTest {

    public static final String SAMPLE_USERNAME = "user@localhost";
    public static final String SAMPLE_SECRET = "secret";
    private IdentityClient identityClient;
    private HttpClient httpClientMock;

    @BeforeEach
    void init() throws ErrorReadingSecretException {
        SecretsReader secretsReaderMock = mock(SecretsReader.class);
        when(secretsReaderMock.fetchSecret(anyString(), anyString())).thenReturn(SAMPLE_SECRET);
        httpClientMock = mock(HttpClient.class);
        identityClient = new IdentityClientImpl(secretsReaderMock, httpClientMock);
    }

    @Test
    void shouldReturnUserResponseWithTwoIncludedUnits() throws IOException, InterruptedException {
        UserResponse userResponse = getUserResponse();
        prepareOkResponse(userResponse);

        Optional<UserResponse> user = identityClient.getUser(SAMPLE_USERNAME);
        assertThat(user.isPresent(), is(equalTo(true)));
        var actualIncludedUnits = user.get().getViewingScope().getIncludedUnits();
        var expectedIncludedUnits = userResponse.getViewingScope().getIncludedUnits();
        assertThat(actualIncludedUnits, is(equalTo(expectedIncludedUnits)));
    }

    @Test
    void shouldReturnOptionalEmptyWhenUserNotFound() throws IOException, InterruptedException {
        prepareNotFoundResponse();

        Optional<UserResponse> user = identityClient.getUser(SAMPLE_USERNAME);
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
        UserResponse userResponse = new UserResponse();
        UserResponse.ViewingScope viewingScope = new UserResponse.ViewingScope();
        viewingScope.setIncludedUnits(Set.of(randomUri(), randomUri()));
        viewingScope.setExcludedUnits(Collections.emptySet());
        userResponse.setViewingScope(viewingScope);
        return userResponse;
    }

    @Test
    void shouldLogMessageWhenSecretsFailToRead() throws ErrorReadingSecretException {
        final TestAppender appender = LogUtils.getTestingAppenderForRootLogger();
        SecretsReader secretsReader = failingSecretsReader();
        HttpClient httpClient = HttpClient.newBuilder().build();
        Executable action = () -> new IdentityClientImpl(secretsReader, httpClient);
        assertThrows(RuntimeException.class, action);
        assertThat(appender.getMessages(), containsString(ERROR_READING_SECRETS_ERROR));
    }

    private SecretsReader failingSecretsReader() throws ErrorReadingSecretException {
        SecretsReader secretsReader = mock(SecretsReader.class);
        when(secretsReader.fetchSecret(anyString(), anyString())).thenThrow(new ErrorReadingSecretException());
        return secretsReader;
    }
}