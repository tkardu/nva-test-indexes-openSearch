package no.unit.nva.indexing.handlers;

import static no.unit.nva.indexing.handlers.IndexCreationHandler.SUCCESS_RESPONSE;
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
import no.unit.nva.search.IndicesClientWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class IndexCreationHandlerTest {

    private IndexCreationHandler indexCreationHandler;
    private IndexingClient indexingClient;
    private Context context;

    @BeforeEach
    void init() {
        indexingClient = mock(IndexingClient.class);
        indexCreationHandler = new IndexCreationHandler(indexingClient);
        context = mock(Context.class);
    }

    @Test
    void shouldNotThrowExceptionIfIndicesClientDoesNotThrowException() throws IOException {
        var indicesClientWrapper = mock(IndicesClientWrapper.class);
        when(indexingClient.getIndicesClientWrapper()).thenReturn(indicesClientWrapper);
        doNothing().when(indexingClient).createIndexBasedOnName(any(String.class), any(IndicesClientWrapper.class));
        var response = indexCreationHandler.handleRequest(null, context);
        assertEquals(response, SUCCESS_RESPONSE);
    }

    @Test
    void shouldThrowExceptionWhenIndexingClientFailedToCreateIndex() throws IOException {
        String expectedMessage = randomString();
        var indicesClientWrapper = mock(IndicesClientWrapper.class);
        when(indexingClient.getIndicesClientWrapper()).thenReturn(indicesClientWrapper);
        when(indexingClient.createIndexBasedOnName(any(String.class), any(IndicesClientWrapper.class))).thenThrow(
            new IOException(expectedMessage));
        Executable handleRequest = () -> indexCreationHandler.handleRequest(null, context);

        var response = assertThrows(RuntimeException.class, handleRequest);
        assertThat(response.getMessage(), containsString(expectedMessage));
    }
}