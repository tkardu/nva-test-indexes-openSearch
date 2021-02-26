package no.unit.nva.search.exception;

import nva.commons.apigateway.exceptions.ApiGatewayException;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ExceptionsTest {

    public static final String MESSAGE = "Message";

    @Test
    void importExceptionHasStatusCode() {
        ApiGatewayException exception = new ImportException(MESSAGE);
        Assertions.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, exception.getStatusCode());
    }

    @Test
    void inputExceptionHasStatusCode() {
        ApiGatewayException exception = new InputException(MESSAGE);
        Assertions.assertEquals(HttpStatus.SC_BAD_REQUEST, exception.getStatusCode());
    }

    @Test
    void searchExceptionHasStatusCode() {
        ApiGatewayException exception = new SearchException(MESSAGE, new RuntimeException());
        Assertions.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, exception.getStatusCode());
    }



}