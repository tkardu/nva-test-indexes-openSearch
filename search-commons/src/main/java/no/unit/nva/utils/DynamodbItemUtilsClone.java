package no.unit.nva.utils;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.model.Publication;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.util.BinaryUtils.copyAllBytesFrom;

/**
 * This methods in this class is lend from com.amazonaws.services.dynamodbv2.document.ItemUtils
 *  *Without* exceptions when class is not recognized
 */
public final class DynamodbItemUtilsClone {

    private static final Logger logger = LoggerFactory.getLogger(DynamodbItemUtilsClone.class);

    private static final ObjectMapper objectMapper = JsonUtils.objectMapper;
    private static final JavaType PARAMETRIC_TYPE =
            objectMapper.getTypeFactory().constructParametricType(Map.class, String.class, AttributeValue.class);
    public static final String ATTRIBUTE_VALUE_MUST_NOT_BE_EMPTY_MESSAGE = "Attribute value must not be empty: {}";

    @JacocoGenerated
    private DynamodbItemUtilsClone() {
    }

    /**
     * Creates an publication from json source.
     * @param serializedDynamoDBRecord json representation of publication.
     * @return Publication created from source
     * @throws JsonProcessingException when there are errors in reading json source
     */
    public static Publication dynamodbSerializedRecordStringToPublication(String serializedDynamoDBRecord)
            throws JsonProcessingException {
        var attributeMap = attributeMapFromDynamoDBSource(serializedDynamoDBRecord);
        Item item = toItem(attributeMap);
        return objectMapper.readValue(item.toJSON(), Publication.class);
    }

    /**
     * Transforms attributeValue to simple json value.
     * @param value simple attributevalue to be transformed
     * @param <T> Generic return type
     * @return value in attributevalue
     */
    @SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops", "PMD.CognitiveComplexity"})
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
            logger.warn(ATTRIBUTE_VALUE_MUST_NOT_BE_EMPTY_MESSAGE,value);
            return null;
        }
    }

    /**
     * Creates an map of attributevalues from JSON source.
     * @param dynamoDbJson  json source containing publication
     * @return map of attributevalues created from json source
     * @throws JsonProcessingException when source contains errors
     */
    private static Map<String, AttributeValue> attributeMapFromDynamoDBSource(String dynamoDbJson)
            throws JsonProcessingException {
        return objectMapper.readValue(dynamoDbJson, PARAMETRIC_TYPE);
    }

    /**
     * Traverses a map containing attributevalues and transforms attributevalues to simple json values.
     * @param values map of attributevalues
     * @param <T> Type to convert to
     * @return simplified map of values
     */
    private static <T> Map<String, T> toSimpleMapValue(Map<String, AttributeValue> values) {
        Map<String, T> result = new LinkedHashMap<>(values.size());
        for (Map.Entry<String, AttributeValue> entry : values.entrySet()) {
            T t = toSimpleValue(entry.getValue());
            result.put(entry.getKey(), t);
        }
        return result;
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
}
