package no.unit.nva.utils;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.internal.ItemValueConformer;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.model.Publication;
import nva.commons.utils.JacocoGenerated;
import nva.commons.utils.JsonUtils;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.util.BinaryUtils.copyAllBytesFrom;

public final class DynamodbExportFormatTransformer {

    private static final ObjectMapper objectMapper = JsonUtils.objectMapper;
    private static final JavaType PARAMETRIC_TYPE =
            objectMapper.getTypeFactory().constructParametricType(Map.class, String.class, AttributeValue.class);
    private static final String SERIALIZED_BOOLEAN_TYPE_TAG = "bOOL";
    private static final String ATTRIBUTE_VALUE_BOOLEAN_TYPE_TAG = "bool";
    private static final ItemValueConformer valueConformer = new ItemValueConformer();

    @JacocoGenerated
    private DynamodbExportFormatTransformer() {
    }


    /**
     * Creates an map of attributevalues from json source.
     * @param dynamoDbJson  json source containing publication
     * @return map of attributevalues created from json source
     * @throws JsonProcessingException when source contains errors
     */
    public static Map<String, AttributeValue> attributeMapFromDynamoDBSource(String dynamoDbJson)
            throws JsonProcessingException {
        return objectMapper.readValue(dynamoDbJson, PARAMETRIC_TYPE);
    }

    /**
     * Fixes problem with boolean values in source.
     * @param source json source possible containing definition of boolean attributevalues
     * @return json source with parsable attributevalues
     */
    public static String fixupBooleanAttributeValue(String source) {
        return source.replace(SERIALIZED_BOOLEAN_TYPE_TAG, ATTRIBUTE_VALUE_BOOLEAN_TYPE_TAG);
    }


    /**
     * Traverses a map containing attributevalues and transforms attributevalues to simple json values.
     * @param values map of attributevalues
     * @param <T> Generic type to convert to
     * @return simplified map of values
     */
    public static <T> Map<String, T> toSimpleMapValue(Map<String, AttributeValue> values) {
        Map<String, T> result = new LinkedHashMap<>(values.size());
        for (Map.Entry<String, AttributeValue> entry : values.entrySet()) {
            T t = toSimpleValue(entry.getValue());
            result.put(entry.getKey(), t);
        }
        return result;
    }

    /**
     * Transforms attributeValue to simple json value.
     * @param value simple attributevalue to be transformed
     * @param <T> Generic return type
     * @return value in attributevalue
     */
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    @JacocoGenerated
    public static <T> T toSimpleValue(AttributeValue value) {
        if (Boolean.TRUE.equals(value.getNULL())) {
            return null;
        } else if (Boolean.FALSE.equals(value.getNULL())) {
            throw new UnsupportedOperationException("False-NULL is not supported in DynamoDB");
        } else if (value.getBOOL() != null) {
            T t = (T) value.getBOOL();
            return t;
        } else if (value.getS() != null) {
            T t = (T) value.getS();
            return t;
        } else if (value.getN() != null) {
            T t = (T) new BigDecimal(value.getN());
            return t;
        } else if (value.getB() != null) {
            return (T) copyAllBytesFrom(value.getB());
        } else if (value.getSS() != null) {
            T t = (T) new LinkedHashSet<>(value.getSS());
            return t;
        } else if (value.getNS() != null) {
            Set<BigDecimal> set = new LinkedHashSet<>(value.getNS().size());
            for (String s : value.getNS()) {
                set.add(new BigDecimal(s));
            }
            T t = (T) set;
            return t;
        } else if (value.getBS() != null) {
            Set<byte[]> set = new LinkedHashSet<>(value.getBS().size());
            for (ByteBuffer bb : value.getBS()) {
                set.add(copyAllBytesFrom(bb));
            }
            T t = (T) set;
            return t;
        } else if (value.getL() != null) {
            T t = (T) toSimpleList(value.getL());
            return t;
        } else if (value.getM() != null) {
            T t = (T) toSimpleMapValue(value.getM());
            return t;
        } else {
            System.err.println("Attribute value must not be empty: " + value);
            return null;
        }
    }

    private static List<Object> toSimpleList(List<AttributeValue> attrValues) {
        List<Object> result = new ArrayList<>(attrValues.size());
        for (AttributeValue attrValue : attrValues) {
            Object value = toSimpleValue(attrValue);
            result.add(value);
        }
        return result;
    }

    private static Item toItem(Map<String, AttributeValue> item) {
        return fromMap(toSimpleMapValue(item));
    }

    private static Item fromMap(Map<String, Object> attributes) {
        Item item = new Item();
        for (Map.Entry<String, Object> e : attributes.entrySet()) {
            item.with(e.getKey(), e.getValue());
        }
        return item;
    }


    /**
     * Convenient factory method - instantiates an <code>Item</code> from the
     * given JSON string.
     *
     * @return an <code>Item</code> initialized from the given JSON document or null if the input is null.
     */
    public static Item fromJson(String json) {
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>)
                valueConformer.transform(Jackson.fromJsonString(json, Map.class));
        return fromMap(map);
    }


    /**
     * Creates an publication from json source.
     * @param serializedDynamoDBRecord json representation of publication.
     * @return Publication created from source
     * @throws JsonProcessingException when there are errors in reading json source
     */
    public static Publication dynamodbSerializedRecordStringToPublication(String serializedDynamoDBRecord)
            throws JsonProcessingException {
        var modifiedJson = fixupBooleanAttributeValue(serializedDynamoDBRecord);
        var attributeMap = attributeMapFromDynamoDBSource(modifiedJson);
        Item item = toItem(attributeMap);
        return objectMapper.readValue(item.toJSON(), Publication.class);

    }

    /**
     * Creates an Item from json source.
     * @param serializedDynamoDBRecord json representation of publication.
     * @return Item created from source
     * @throws JsonProcessingException when there are errors in reading json source
     */
    public static Item dynamodbExportFormatToItem(String serializedDynamoDBRecord)
            throws JsonProcessingException {
        var modifiedJson = fixupBooleanAttributeValue(serializedDynamoDBRecord);
        var attributeMap = attributeMapFromDynamoDBSource(modifiedJson);
        return toItem(attributeMap);
    }



}