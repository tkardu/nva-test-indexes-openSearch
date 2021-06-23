package no.unit.nva.utils;

import static nva.commons.core.ioutils.IoUtils.inputStreamFromResources;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;
import no.unit.nva.model.Publication;
import nva.commons.core.JacocoGenerated;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@JacocoGenerated
public class DynamodbItemUtilsCloneTest {

    private static final String ACTUAL_DATAPIPELINE_OUTPUT_FILE = "31184c66-88a6-47f4-86a5-2334a05d87b2";
    private static final int EXPECTED_NUMBER_OF_PUBLICATIONS = 32;
    private static final String SERIALIZED_BOOLEAN_TYPE_TAG = "bOOL";
    private static final String ATTRIBUTE_VALUE_BOOLEAN_TYPE_TAG = "bool";

    @Test
    void readingFromDatapipelineFileUsingToPublication() throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStreamFromResources(ACTUAL_DATAPIPELINE_OUTPUT_FILE)))) {

            //Get Stream with lines from BufferedReader
            var publications = reader.lines()
                    //Gives each line as string to the changeTrumpToDrumpf method of this.
                    .map(this::fixupBooleanAttributeValue)
                    .map(this::toPublication)
                    //Calls for each line the print method of this.
                    .collect(Collectors.toList());

            Assertions.assertEquals(EXPECTED_NUMBER_OF_PUBLICATIONS, publications.size());
        }
    }

    /**
     * Fixes problem with boolean values in source.
     * @param source json source possible containing definition of boolean attributevalues
     * @return json source with parsable attributevalues
     */
    private String fixupBooleanAttributeValue(String source) {
        return source.replace(SERIALIZED_BOOLEAN_TYPE_TAG, ATTRIBUTE_VALUE_BOOLEAN_TYPE_TAG);
    }

    private Publication toPublication(String json) {
        try {
            return DynamodbItemUtilsClone.dynamodbSerializedRecordStringToPublication(json);
        } catch (JsonProcessingException ignored) {
            return null;
        }
    }

}