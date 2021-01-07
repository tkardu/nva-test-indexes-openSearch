package no.unit.nva.search.exception;

import nva.commons.exceptions.ApiGatewayException;
import nva.commons.utils.JacocoGenerated;
import org.apache.http.HttpStatus;

@JacocoGenerated
public class ImportException extends ApiGatewayException {

    public ImportException(String message) {
        super(message);
    }

    @Override
    protected Integer statusCode() {
        return HttpStatus.SC_INTERNAL_SERVER_ERROR;
    }
}
