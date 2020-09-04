package no.unit.nva.search.exception;

import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ExceptionsTest {

    public static final String DUMMY_ERROR_TEXT = "Dummy error text";

    @Test
    public void creatingInputExceptionDefaultConstructorReturnsBadRequest() {
        InputException inputException  = new InputException(DUMMY_ERROR_TEXT, new RuntimeException());
        assertNotNull(inputException);
        assertEquals(HttpStatus.SC_BAD_REQUEST, inputException.statusCode());
        assertEquals(DUMMY_ERROR_TEXT, inputException.getMessage());
    }


    @Test
    public void creatingSearchExceptionDefaultConstructorReturnsbadRequest() {
        SearchException searchException  = new SearchException(DUMMY_ERROR_TEXT, new RuntimeException());
        assertNotNull(searchException);
        assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, searchException.statusCode());
        assertEquals(DUMMY_ERROR_TEXT, searchException.getMessage());
    }


}
