package no.unit.nva.search.models;

import static no.unit.nva.search.IndexingConfig.objectMapper;
import static no.unit.nva.testutils.RandomDataGenerator.randomJson;
import static no.unit.nva.testutils.RandomDataGenerator.randomString;
import static nva.commons.core.attempt.Try.attempt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.stream.Stream;
import no.unit.nva.identifiers.SortableIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class IndexDocumentTest {

    public static Stream<IndexDocument> invalidConsumptionAttributes() {
        var consumptionAttributesMissingIndexName = new EventConsumptionAttributes(null,SortableIdentifier.next());
        var consumptionAttributesMissingDocumentIdentifier =
            new EventConsumptionAttributes(randomString(),null);
        return Stream.of(consumptionAttributesMissingIndexName,consumptionAttributesMissingDocumentIdentifier)
                   .map(consumptionAttributes->new IndexDocument(consumptionAttributes,randomJsonObject()));
    }

    @Test
    void shouldReturnElasticSearchIndexRequestWithIndexNameSpecifiedByConsumptionAttributes()
        throws JsonProcessingException {
        var consumptionAttributes = randomConsumptionAttributes();
        var indexDocument = new IndexDocument(consumptionAttributes, randomJsonObject());
        var indexRequest = indexDocument.toIndexRequest();
        assertThat(indexRequest.index(), is(equalTo(consumptionAttributes.getIndex())));
    }

    @Test
    void shouldReturnObjectWhenInputIsValidJsonString() throws JsonProcessingException {
        var indexDocument = new IndexDocument(randomConsumptionAttributes(), randomJsonObject());
        var json = objectMapper.writeValueAsString(indexDocument);
        var deserialized = IndexDocument.fromJsonString(json);
        assertThat(deserialized, is(equalTo(indexDocument)));
    }

    @Test
    void shouldReturnDocumentIdentifierOfContainedObjectWhenEventConsumptionAttributesContainIdentifier()
        throws JsonProcessingException {
        var indexDocument = new IndexDocument(randomConsumptionAttributes(), randomJsonObject());
        assertThat(indexDocument.getDocumentIdentifier(),
                   is(equalTo(indexDocument.getConsumptionAttributes().getDocumentIdentifier().toString())));
    }

    @Test
    void shouldThrowExceptionWhenEventConsumptionAttributesDoNotContainIndexName() throws JsonProcessingException {
        var consumptionAttributes = new EventConsumptionAttributes(null, SortableIdentifier.next());
        var indexDocument = new IndexDocument(consumptionAttributes, randomJsonObject());
        var error = assertThrows(RuntimeException.class, indexDocument::getIndexName);
        assertThat(error.getMessage(), containsString(IndexDocument.MISSING_INDEX_NAME_IN_RESOURCE));
    }

    @Test
    void shouldThrowExceptionWhenEventConsumptionAttributesDoNotContainDocumentIdentifier()
        throws JsonProcessingException {
        var consumptionAttributes = new EventConsumptionAttributes(randomString(), null);
        var indexDocument = new IndexDocument(consumptionAttributes, randomJsonObject());
        var error = assertThrows(RuntimeException.class, indexDocument::getDocumentIdentifier);
        assertThat(error.getMessage(), containsString(IndexDocument.MISSING_IDENTIFIER_IN_RESOURCE));
    }

    @ParameterizedTest(name ="should throw exception when validating and missing mandatory fields:{0}")
    @MethodSource("invalidConsumptionAttributes")
    void shouldThrowExceptionWhenValidatingAndMissingMandatoryFields(IndexDocument invalidIndexDocument){
        assertThrows(Exception.class, invalidIndexDocument::validate);
    }

    private static ObjectNode randomJsonObject()  {
        String json = randomJson();
        return attempt(()->(ObjectNode) objectMapper.readTree(json)).orElseThrow();
    }

    private EventConsumptionAttributes randomConsumptionAttributes() {
        return new EventConsumptionAttributes(randomString(), SortableIdentifier.next());
    }
}