package no.unit.nva.search.exception;

public class MissingUuidException extends Exception {

    public static final String NO_PATH_IN_URI = "The input record lacks a Uuid in its URI: ";

    public MissingUuidException(String uri) {
        super(NO_PATH_IN_URI + uri);
    }
}
