package no.unit.nva.search.restclients;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.net.HttpURLConnection.HTTP_OK;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Optional;
import no.unit.nva.search.restclients.responses.UserResponse;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.paths.UriWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdentityClientImpl implements IdentityClient {

    public static final String RESPONSE_STATUS_BODY = "Response status=%s, body=%s";
    public static final String HTTPS_SCHEME = "https";
    public static final String API_HOST = new Environment().readEnv("API_HOST");
    public static final String USER_SERVICE_PATH = "users-roles/users";
    private static final String GET_USER_ERROR = "Error getting customerId from user";
    private final Logger logger = LoggerFactory.getLogger(IdentityClientImpl.class);
    private final HttpClient httpClient;

    @JacocoGenerated
    public IdentityClientImpl() {
        this(HttpClient.newHttpClient());
    }

    public IdentityClientImpl(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public Optional<UserResponse> getUser(String username, String accessToken) {
        UserResponse userResponse = null;
        try {
            HttpRequest request = createGetUserHttpRequest(createGetUserInternalUri(username), accessToken);
            HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());
            if (response.statusCode() == HTTP_OK) {
                userResponse = UserResponse.fromJson(response.body());
            } else {
                logWarning(response);
            }
        } catch (IOException | InterruptedException e) {
            logger.warn(GET_USER_ERROR, e);
        }
        return Optional.ofNullable(userResponse);
    }

    private void logWarning(HttpResponse<String> response) {
        logger.warn(String.format(RESPONSE_STATUS_BODY, response.statusCode(), response.body()));
    }

    private HttpRequest createGetUserHttpRequest(URI getUserUri, String accessToken) {
        return HttpRequest.newBuilder()
            .uri(getUserUri)
            .header(ACCEPT, JSON_UTF_8.toString())
            .header(AUTHORIZATION, accessToken)
            .GET()
            .build();
    }

    private URI createGetUserInternalUri(String username) {
        return new UriWrapper(HTTPS_SCHEME, API_HOST)
            .addChild(USER_SERVICE_PATH)
            .addChild(username)
            .getUri();
    }
}
