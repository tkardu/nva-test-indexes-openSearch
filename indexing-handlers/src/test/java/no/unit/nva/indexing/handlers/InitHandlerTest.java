package no.unit.nva.indexing.handlers;

import static no.unit.nva.indexing.handlers.InitHandler.SUCCESS_RESPONSE;
import static no.unit.nva.testutils.RandomDataGenerator.randomString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.amazonaws.services.lambda.runtime.Context;
import java.io.IOException;
import no.unit.nva.search.IndexingClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class InitHandlerTest {

    private InitHandler initHandler;
    private IndexingClient indexingClient;
    private Context context;

    @BeforeEach
    void init() {
        indexingClient = mock(IndexingClient.class);
        initHandler = new InitHandler(indexingClient);
        context = mock(Context.class);
    }

    @Test
    void shouldNotThrowExceptionIfIndicesClientDoesNotThrowException() throws IOException {
        doNothing().when(indexingClient).createIndex(any(String.class));
        var response = initHandler.handleRequest(null, context);
        assertEquals(response, SUCCESS_RESPONSE);
    }

    @Test
    void shouldThrowExceptionWhenIndexingClientFailedToCreateIndex() throws IOException {
        String expectedMessage = randomString();
        when(indexingClient.createIndex(any(String.class))).thenThrow(
            new IOException(expectedMessage));
        Executable handleRequest = () -> initHandler.handleRequest(null, context);

        var response = assertThrows(RuntimeException.class, handleRequest);
        assertThat(response.getMessage(), containsString(expectedMessage));
    }
}