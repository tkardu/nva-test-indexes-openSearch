package no.unit.nva.utils;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.publication.Contributor;
import no.unit.nva.publication.TestDataGenerator;
import no.unit.nva.search.IndexContributor;
import no.unit.nva.search.IndexDate;
import no.unit.nva.search.IndexDocument;
import no.unit.nva.search.IndexPublisher;
import nva.commons.utils.JsonUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static no.unit.nva.utils.IndexDocumentGenerator.ABSTRACT;
import static no.unit.nva.utils.IndexDocumentGenerator.DESCRIPTION;
import static no.unit.nva.utils.IndexDocumentGenerator.PUBLISHED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;



class IndexDocumentGeneratorTest {

    public static final int NUMBER_OF_CONTRIBUTOR_IRIS_IN_SAMPLE = 2;
    public static final ObjectMapper mapper = JsonUtils.objectMapper;
    public static final String MODIFY = "MODIFY";
    public static final String EXAMPLE_ARP_URI_BASE = "https://example.org/arp/";
    public static final String EVENT_ID = "eventID";
    public static final String OWNER = "jd@not.here";
    private static final URI SAMPLE_DOI = URI.create("https://doi.org/10.1103/physrevd.100.085005");
    private static final URI SAMPLE_PUBLISHER_ID =
            URI.create("https://api.dev.nva.aws.unit.no/customer/f54c8aa9-073a-46a1-8f7c-dde66c853934");
    private static final String SAMPLE_PUBLISHER_NAME = "Organization";
    private static final IndexPublisher SAMPLE_PUBLISHER = new IndexPublisher.Builder()
            .withId(SAMPLE_PUBLISHER_ID).withName(SAMPLE_PUBLISHER_NAME).build();
    public static final Instant SAMPLE_MODIFIED_DATE = Instant.now();
    public static final Instant SAMPLE_PUBLISHED_DATE = Instant.now();


    @Test
    void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithMultipleContributorsWhenContributorIdIsIRI()
            throws IOException {
        var dynamoDbStreamRecord =
                new TestDataGenerator.Builder().build().getSampleDynamoDBStreamRecord();
        IndexDocument document = IndexDocumentGenerator.fromJsonNode(dynamoDbStreamRecord);
        assertNotNull(document);

        List<IndexContributor> indexContributors = document.getContributors();
        var ids = indexContributors.stream()
                .map(IndexContributor::getId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        assertThat("Contributors has id", ids.size() == NUMBER_OF_CONTRIBUTOR_IRIS_IN_SAMPLE);
    }

    @Test
    @DisplayName("Test dynamoDBStreamHandler with complete record, single author")
    void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithContributorsWhenInputIsModifyEvent()
            throws IOException {
        TestDataGenerator testData = generateTestDataWithSingleContributor();

        JsonNode requestBody = extractRequestBodyFromEvent(testData.asDynamoDbEvent());

        IndexDocument expected = testData.asIndexDocument();
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);

        assertThat(actual, equalTo(expected));
    }

    private TestDataGenerator generateTestDataWithSingleContributor() throws IOException {
        UUID id = generateValidId();
        String contributorIdentifier = "123";
        String contributorName = "Bólsön Kölàdỳ";
        List<Contributor> contributors = Collections.singletonList(
                generateContributor(contributorIdentifier, contributorName, 1));
        String mainTitle = "Moi buki";
        String type = "Book";
        IndexDate date = new IndexDate("2020", "09", "08");

        return new TestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withStatus(PUBLISHED)
                .withEventName(MODIFY)
                .withId(id)
                .withType(type)
                .withTitle(mainTitle)
                .withContributors(contributors)
                .withDate(date)
                .withOwner(OWNER)
                .withDescription(DESCRIPTION)
                .withAbstract(ABSTRACT)
                .withDoi(SAMPLE_DOI)
                .withPublisher(SAMPLE_PUBLISHER)
                .withModifiedDate(SAMPLE_MODIFIED_DATE)
                .withPublishedDate(SAMPLE_PUBLISHED_DATE)
                .build();
    }

    private Contributor generateContributor(String identifier, String name, int sequence) {
        return new Contributor(sequence, name, identifier, URI.create(EXAMPLE_ARP_URI_BASE + identifier));
    }

    private UUID generateValidId() {
        return UUID.randomUUID();
    }

    private List<Contributor> generateContributors() {
        String firstContributorIdentifier = "123";
        String firstContributorName = "Bólsön Kölàdỳ";
        String secondContributorIdentifier = "345";
        String secondContributorName = "Mèrdok Hüber";
        List<Contributor> contributors = new ArrayList<>();
        contributors.add(generateContributor(firstContributorIdentifier, firstContributorName, 1));
        contributors.add(generateContributor(secondContributorIdentifier, secondContributorName, 2));
        return contributors;
    }

    private JsonNode extractRequestBodyFromEvent(DynamodbEvent requestEvent) {
        IndexDocument indexDocument = IndexDocumentGenerator
                .fromStreamRecord(requestEvent.getRecords().get(0))
                .toIndexDocument();
        return mapper.valueToTree(indexDocument);
    }

}