package no.unit.nva.search.exception;

import no.unit.nva.search.SearchResourcesApiHandler;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class InputExceptionTest {

    @Test
    @DisplayName("testInputExceptionDefaultConstructorReturnsbadRequest")
    public void testInputExceptionDefaultConstructorReturnsbadRequest() {
        InputException inputException  = new InputException("Dummy error text", new RuntimeException());
        assertNotNull(inputException);
        assertEquals(HttpStatus.SC_BAD_REQUEST, inputException.statusCode());
    }


}
