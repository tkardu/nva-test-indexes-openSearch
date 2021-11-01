package no.unit.nva.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static no.unit.nva.search.constants.ApplicationConstants.objectMapperNoEmpty;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.StringContains.containsString;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class ImportDataRequestTest {

    private static final String SOME_S3_LOCATION = "s3://some-bucket/some/path";

    @Test
    public void creatorReturnsValidObjectWhenInputIsNotEmpty() {
        ImportDataRequest request = new ImportDataRequest(SOME_S3_LOCATION);
        assertThat(request.getS3Location(), is(equalTo(SOME_S3_LOCATION)));
    }

    @Test
    public void creatorThrowsExceptionWhenInputIsInvalid() {
        ObjectNode objectNode = objectMapperNoEmpty.createObjectNode();
        String jsonString = objectNode.toPrettyString();
        Executable action = () -> objectMapperWithEmpty.readValue(jsonString, ImportDataRequest.class);
        ValueInstantiationException exception = assertThrows(ValueInstantiationException.class, action);
        assertThat(exception.getMessage(), containsString(ImportDataRequest.S3_LOCATION_FIELD));
    }

    @Test
    public void serializationWithJsonReturnsValidObject() throws JsonProcessingException {
        ObjectNode objectNode = objectMapperNoEmpty.createObjectNode();
        String jsonString = objectNode.put(ImportDataRequest.S3_LOCATION_FIELD, SOME_S3_LOCATION).toPrettyString();
        ImportDataRequest deserialized = objectMapperWithEmpty.readValue(jsonString, ImportDataRequest.class);
        assertThat(deserialized.getS3Location(), is(equalTo(SOME_S3_LOCATION)));
    }

    @Test
    public void getPathReturnsPathWithoutRoot() {
        ImportDataRequest request = new ImportDataRequest(SOME_S3_LOCATION);
        assertThat(request.getS3Path(), not(startsWith(ImportDataRequest.PATH_DELIMITER)));
    }
}
