package no.unit.nva.search.exception;

import nva.commons.exceptions.ApiGatewayException;
import nva.commons.utils.JacocoGenerated;
import org.apache.http.HttpStatus;

@JacocoGenerated
public class SearchException extends ApiGatewayException {

    public SearchException(String message, Exception exception) {
        super(exception, message);
    }

    @Override
    protected Integer statusCode() {
        return HttpStatus.SC_INTERNAL_SERVER_ERROR;
    }
}
