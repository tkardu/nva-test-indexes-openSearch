package no.unit.nva.search.restclients;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.net.HttpURLConnection.HTTP_OK;
import static no.unit.nva.search.constants.ApplicationConstants.ENVIRONMENT;
import static nva.commons.core.attempt.Try.attempt;
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
import nva.commons.secrets.SecretsReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdentityClientImpl implements IdentityClient {

    public static final String ERROR_READING_SECRETS_ERROR =
        "Could not read secrets for internal communication with identity service";
    private static final String GET_USER_ERROR = "Error getting customerId from user";
    public static final String RESPONSE_STATUS_BODY = "Response status=%s, body=%s";
    public static final String CREATING_REQUEST_TO = "Creating request to: ";
    public static final String HTTPS_SCHEME = "https";
    public static final String IDENTITY_SERVICE_SECRET_NAME = ENVIRONMENT
            .readEnv("IDENTITY_SERVICE_SECRET_NAME");
    public static final String IDENTITY_SERVICE_SECRET_KEY = ENVIRONMENT
            .readEnv("IDENTITY_SERVICE_SECRET_KEY");
    public static final String API_HOST = new Environment().readEnv("API_HOST");
    public static final String USER_INTERNAL_SERVICE_PATH = "identity-internal/user";

    private final Logger logger = LoggerFactory.getLogger(IdentityClientImpl.class);
    private final HttpClient httpClient;
    private final String identityServiceSecret;

    public IdentityClientImpl(SecretsReader secretsReader, HttpClient httpClient) {
        this.httpClient = httpClient;
        this.identityServiceSecret = attempt(() -> fetchSecret(secretsReader))
            .orElseThrow(fail -> logAndFail(fail.getException()));
    }

    @JacocoGenerated
    public IdentityClientImpl() {
        this(new SecretsReader(), HttpClient.newHttpClient());
    }

    @Override
    public Optional<UserResponse> getUser(String username) {
        UserResponse userResponse = null;
        try {
            HttpRequest request = createGetUserHttpRequest(createGetUserInternalUri(username));
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

    private RuntimeException logAndFail(Exception exception) {
        logger.error(ERROR_READING_SECRETS_ERROR);
        return new RuntimeException(exception);
    }

    private String fetchSecret(SecretsReader secretsReader) {
        return secretsReader.fetchSecret(IDENTITY_SERVICE_SECRET_NAME, IDENTITY_SERVICE_SECRET_KEY);
    }

    private HttpRequest createGetUserHttpRequest(URI getUserUri) {
        logger.info(CREATING_REQUEST_TO + getUserUri);
        return HttpRequest.newBuilder()
            .uri(getUserUri)
            .headers(ACCEPT, JSON_UTF_8.toString(), AUTHORIZATION, identityServiceSecret)
            .GET()
            .build();
    }

    private URI createGetUserInternalUri(String username) {
        return new UriWrapper(HTTPS_SCHEME, API_HOST)
            .addChild(USER_INTERNAL_SERVICE_PATH)
            .addChild(username)
            .getUri();
    }
}
