package no.unit.nva.search.exception;

public class MalformedUuidException extends Exception {

    public static final String NOT_A_UUID_MESSAGE = "The given string is not a UUID: ";

    public MalformedUuidException(String pathPart) {
        super(NOT_A_UUID_MESSAGE + pathPart);
    }
}
