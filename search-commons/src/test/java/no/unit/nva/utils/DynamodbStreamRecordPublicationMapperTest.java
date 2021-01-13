package no.unit.nva.utils;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.model.Publication;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.utils.IoUtils;
import nva.commons.utils.JsonUtils;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.stream.Collectors;

import static no.unit.nva.utils.DynamodbStreamRecordPublicationMapper.toSimpleMapValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DynamodbStreamRecordPublicationMapperTest {

    public static final String ERROR_MAPPING_ITEM_TO_PUBLICATION = "Error mapping Item to Publication";
    public static final String ERROR_MAPPING_PUBLICATION_TO_ITEM = "Error mapping Publication to Item";
    private static final String PUBLICATION_JSON_FILE = "publication.json";
    private static final String SAMPLE_DYNAMODB_RECORD = "actual_dynamodb_record.json";
    private static final String EDITED_SAMPLE_DYNAMODB_RECORD = "actual_dynamodb_record.json";
    private static final ObjectMapper objectMapper = JsonUtils.objectMapper;
    public static final String SIMPLE_STRING_VALUE = "simple string value";
    private static final String SAMPLE_DATAPIPELINE_OUTPUT_FILE = "datapipeline_output_sample.json";
    private static final String ACTUAL_DATAPIPELINE_OUTPUT_FILE = "31184c66-88a6-47f4-86a5-2334a05d87b2";

    @Test
    void publicationToItemAndBack() throws Exception {
        var expectedPublication = getPublication();
        var item = publicationToItem(expectedPublication);
        var actualPublication = itemToPublication(item);
        assertEquals(expectedPublication, actualPublication);
    }

    @Test
    void publicationToItemAndMapAndBack() throws Exception {
        var expectedPublication = getPublication();
        var item = publicationToItem(expectedPublication);
        var map = ItemUtils.toAttributeValues(item);
        var itemFromMap = ItemUtils.toItem(map);
        var actualPublication = itemToPublication(itemFromMap);
        assertEquals(expectedPublication, actualPublication);
    }

    @Test
    void publicationToItemAndMapAttributeValueAndJsonAndBack() throws Exception {
        var expectedPublication = getPublication();
        var item = publicationToItem(expectedPublication);
        var map = ItemUtils.toAttributeValues(item);
        var json = map.toString();
        System.out.println("json=" + json);
        var itemFromMap = ItemUtils.toItem(map);
        var actualPublication = itemToPublication(itemFromMap);
        assertEquals(expectedPublication, actualPublication);
    }

    @Test
    void publicationToItemAndMapObjectAndBack() throws Exception {
        var expectedPublication = getPublication();
        var item = publicationToItem(expectedPublication);
        var map = item.asMap();
        var json = map.toString();
        System.out.println("json=" + json);
        var itemFromMap = Item.fromMap(map);
        var actualPublication = itemToPublication(itemFromMap);
        assertEquals(expectedPublication, actualPublication);
    }

    @Test
    void publicationToItemAndMapValueAttributeToSimpleTypeAndBack() throws Exception {
        var expectedPublication = getPublication();
        var item = publicationToItem(expectedPublication);
        var mapOfAttriubuteValues = ItemUtils.toAttributeValues(item);
        var dynamoDbjson = mapOfAttriubuteValues.toString();
        System.out.println("dynamoDbjson=" + dynamoDbjson);

        var mapOfObjects = normalizeAttributeValues(mapOfAttriubuteValues);
        var json = mapOfObjects.toString();
        System.out.println("json=" + json);
        var itemFromMap = Item.fromMap(mapOfObjects);
        var actualPublication = itemToPublication(itemFromMap);
        assertEquals(expectedPublication, actualPublication);
    }

    @Test
    void publicationFromMapValueAttribute() throws Exception {
        var expectedPublication = getPublication();
        var item = publicationToItem(expectedPublication);
        var mapOfAttriubuteValues = ItemUtils.toAttributeValues(item);
        var actualPublication = fromDynamoDbAttributeMap(mapOfAttriubuteValues);

        assertEquals(expectedPublication, actualPublication);
    }

    private Map<String, Object> normalizeAttributeValues(Map<String, AttributeValue> dynamoDbMap) {
        Map<String, Object> normalizedValues = toSimpleMapValue(dynamoDbMap);
        return normalizedValues;
    }

    private Publication fromDynamoDbAttributeMap(Map<String, AttributeValue> mapOfAttributeValues)
            throws ApiGatewayException {
        var dynamoDbjson = mapOfAttributeValues.toString();
        System.out.println("dynamoDbjson=" + dynamoDbjson);

        var mapOfObjects = normalizeAttributeValues(mapOfAttributeValues);
        var json = mapOfObjects.toString();
        System.out.println("json=" + json);
        var itemFromMap = Item.fromMap(mapOfObjects);
        var actualPublication = itemToPublication(itemFromMap);
        return actualPublication;
    }

    @Test
    void converterCreatesPublicationFromJsonGeneratedFromPublication() throws JsonProcessingException,
            ApiGatewayException {
        var expectedPublication = getPublication();

        var item = publicationToItem(expectedPublication);
        var actualPublication = itemToPublication(item);

        assertEquals(expectedPublication, actualPublication);
    }

    private Publication getPublication() throws JsonProcessingException {
        String dynamoDBJsonSample = IoUtils.streamToString(IoUtils.inputStreamFromResources(PUBLICATION_JSON_FILE));
        var publication = objectMapper.readValue(dynamoDBJsonSample, Publication.class);
        return publication;
    }

    protected Item publicationToItem(Publication publication) throws ApiGatewayException {
        Item item;
        try {
            item = Item.fromJSON(objectMapper.writeValueAsString(publication));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(ERROR_MAPPING_PUBLICATION_TO_ITEM, e);
        }
        return item;
    }

    protected Publication itemToPublication(Item item) throws ApiGatewayException {
        Publication publicationOutcome;
        try {
            publicationOutcome = objectMapper.readValue(item.toJSON(), Publication.class);
        } catch (Exception e) {
            throw new RuntimeException(ERROR_MAPPING_ITEM_TO_PUBLICATION, e);
        }
        return publicationOutcome;
    }


    @Test
    void converterCreatesStringFromSimpleSource() throws JsonProcessingException {
        var simpleStringAttributeValue = "{  's': '" + SIMPLE_STRING_VALUE + "' } ";
        var attributeValue = objectMapper.readValue(simpleStringAttributeValue, AttributeValue.class);
        var simpleValue = ItemUtils.toSimpleValue(attributeValue);
        assertEquals(SIMPLE_STRING_VALUE, simpleValue);
    }

    @Test
    void converterCreatesStringFromSimpleSourceMap() throws JsonProcessingException {
        var simpleStringAttributeValue = "{ \"value\" : { 's': '" + SIMPLE_STRING_VALUE + "' } } ";

        var javaType = objectMapper.getTypeFactory().constructParametricType(Map.class, String.class,
                com.amazonaws.services.dynamodbv2.model.AttributeValue.class);

        Map<String, AttributeValue> mappedAttributeMap = objectMapper.readValue(simpleStringAttributeValue, javaType);
        System.out.println("mappedAttributeMap=" + mappedAttributeMap);

        assertEquals(SIMPLE_STRING_VALUE, ItemUtils.toSimpleValue(mappedAttributeMap.get("value")));
    }

    @Test
    void converterCreatesBooleanFromSimpleSource() throws JsonProcessingException {
        var simpleStringAttributeValue = "{'bool' : false }";

        AttributeValue attributeValue = objectMapper.readValue(simpleStringAttributeValue, AttributeValue.class);
        System.out.println("attributeValue=" + attributeValue);

        assertEquals(Boolean.FALSE, ItemUtils.toSimpleValue(attributeValue));
    }

    @Test
    void readingFromBooleanFixedFile() throws JsonProcessingException {

        var rawjson = IoUtils.streamToString(IoUtils.inputStreamFromResources(SAMPLE_DATAPIPELINE_OUTPUT_FILE));
        var publication = DynamodbStreamRecordPublicationMapper
                .dynamodbSerializedRecordStringToPublication(rawjson);
        assertNotNull(publication);
        System.out.println("publication=" + publication);

    }


    private Publication toPublication(String json) {
        try {
            return DynamodbStreamRecordPublicationMapper.dynamodbSerializedRecordStringToPublication(json);
        } catch (JsonProcessingException e) {
            System.out.println("json=" + json);
            e.printStackTrace();
            return null;
        }
    }

    @Test
    void readingFromDatapipelineFileUsingToPublication() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(IoUtils.inputStreamFromResources(ACTUAL_DATAPIPELINE_OUTPUT_FILE)))) {

            //Get Stream with lines from BufferedReader
            var publications = reader.lines()
                    //Gives each line as string to the changeTrumpToDrumpf method of this.
                    .map(this::toPublication)
                    //Calls for each line the print method of this.
                    .collect(Collectors.toList());

            publications.forEach(this::print);

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

    }

    @Test
    void readingFromDatapipelineFileUsingItem() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(IoUtils.inputStreamFromResources(ACTUAL_DATAPIPELINE_OUTPUT_FILE)))) {

            //Get Stream with lines from BufferedReader
            var publications = reader.lines()
                    //Gives each line as string to the changeTrumpToDrumpf method of this.
                    .map(this::jsonToItem)
                    //Calls for each line the print method of this.
                    .collect(Collectors.toList());

            publications.forEach(this::print);

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

    }

    private void print(Publication publication) {
        try {
            System.out.println(objectMapper.writeValueAsString(publication));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        ;
    }

    private Publication jsonToItem(String serializedDynamoDBRecord) {

        var modifiedJson = DynamodbStreamRecordPublicationMapper
                .fixupBooleanAttributeValue(serializedDynamoDBRecord);
        System.out.println(modifiedJson);
        var item = DynamodbStreamRecordPublicationMapper.fromJson(modifiedJson);
        Publication publication = null;
        try {
            publication = objectMapper.readValue(item.toJSON(), Publication.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return publication;

    }




}