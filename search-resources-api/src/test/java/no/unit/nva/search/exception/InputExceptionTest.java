package no.unit.nva.search.exception;

import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class InputExceptionTest {

    public static final String DUMMY_ERROR_TEXT = "Dummy error text";

    @Test
    public void creatingInputExceptionDefaultConstructorReturnsbadRequest() {
        InputException inputException  = new InputException(DUMMY_ERROR_TEXT, new RuntimeException());
        assertNotNull(inputException);
        assertEquals(HttpStatus.SC_BAD_REQUEST, inputException.statusCode());
        assertEquals(DUMMY_ERROR_TEXT, inputException.getMessage());
    }


}
