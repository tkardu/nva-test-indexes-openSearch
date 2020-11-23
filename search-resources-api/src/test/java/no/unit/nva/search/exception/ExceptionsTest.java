package no.unit.nva.search.exception;

import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;


public class ExceptionsTest {

    public static final String DUMMY_ERROR_TEXT = "Dummy error text";

    @Test
    public void creatingInputExceptionDefaultConstructorReturnsBadRequest() {
        InputException invalidInputException = new InputException(DUMMY_ERROR_TEXT);
        assertThat(invalidInputException.getStatusCode(), is(equalTo(HttpStatus.SC_BAD_REQUEST)));
    }

    @Test
    public void creatingSearchExceptionDefaultConstructorReturnsbadRequest() {
        SearchException searchException  = new SearchException(DUMMY_ERROR_TEXT, new RuntimeException());
        assertThat(searchException.getStatusCode(), is(equalTo(HttpStatus.SC_INTERNAL_SERVER_ERROR)));
    }

    @Test
    public void creatingImportExceptionDefaultConstructorReturnsbadRequest() {
        ImportException importException  = new ImportException(DUMMY_ERROR_TEXT);
        assertThat(importException.getStatusCode(), is(equalTo(HttpStatus.SC_INTERNAL_SERVER_ERROR)));
    }



}
