package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.model.Reference;
import no.unit.nva.model.contexttypes.Book;
import no.unit.nva.model.contexttypes.BookSeries;
import no.unit.nva.model.contexttypes.PublicationContext;
import no.unit.nva.model.contexttypes.PublishingHouse;
import no.unit.nva.model.contexttypes.Series;
import no.unit.nva.model.exceptions.InvalidIsbnException;
import no.unit.nva.model.exceptions.InvalidUnconfirmedSeriesException;
import no.unit.nva.model.instancetypes.PublicationInstance;
import no.unit.nva.model.instancetypes.book.BookMonograph;
import nva.commons.core.JsonUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static no.unit.nva.publication.PublicationGenerator.randomISBN;
import static no.unit.nva.publication.PublicationGenerator.randomPublicationChannelsUri;
import static no.unit.nva.publication.PublicationGenerator.publishingHouseWithUri;
import static no.unit.nva.publication.PublicationGenerator.randomString;
import static no.unit.nva.search.IndexDocumentGenerator.ABSTRACT;
import static no.unit.nva.search.IndexDocumentGenerator.DESCRIPTION;
import static no.unit.nva.search.IndexDocumentGenerator.PUBLISHED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class IndexDocumentGeneratorTest {

    public static final int NUMBER_OF_CONTRIBUTOR_IRIS_IN_SAMPLE = 2;
    public static final ObjectMapper mapper = JsonUtils.objectMapperWithEmpty;
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
    private static final Instant SAMPLE_MODIFIED_DATE = Instant.now();
    private static final Instant SAMPLE_PUBLISHED_DATE = Instant.now();
    private static final Map<String, String> SAMPLE_ALTERNATIVETITLES  = Map.of("a", "b","c", "d");
    private static final List<String> SAMPLE_TAGS = List.of("tag1", "tag2");

    private static Reference createReference() throws InvalidIsbnException, InvalidUnconfirmedSeriesException {
        PublicationInstance publicationInstance = new BookMonograph.Builder().build();
        BookSeries series = new Series(randomPublicationChannelsUri());
        final String seriesNumber = randomString();
        final PublishingHouse publisher = publishingHouseWithUri();
        final List<String> isbnList = List.of(randomISBN());
        PublicationContext  publicationContext = new Book(series, seriesNumber, publisher, isbnList);

        return new Reference.Builder()
                .withPublicationInstance(publicationInstance)
                .withPublishingContext(publicationContext)
                .withDoi(SAMPLE_DOI)
                .build();
    }

    @Test
    void publicationIndexEventHandlerCreatesHttpRequestWithIndexDocumentWithMultipleContributorsWhenContributorIdIsIRI()
            throws IOException {
        var dynamoDbStreamRecord =
                new DynamoDBTestDataGenerator.Builder().build().getSampleDynamoDBStreamRecord();
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
    void publicationUpdateEventHandlerCreatesHttpRequestWithIndexDocumentWithContributorsWhenInputIsModifyEvent()
            throws IOException, InvalidIsbnException, InvalidUnconfirmedSeriesException {
        DynamoDBTestDataGenerator testData = generateTestDataWithSingleContributorTDG();

        JsonNode requestBody = extractRequestBodyFromEvent(testData.asDynamoDbEvent());

        IndexDocument expected = testData.asIndexDocument();
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    void indexDocumentGeneratorHandlesMissingFieldsAndWritesToLog()
            throws IOException, InvalidIsbnException, InvalidUnconfirmedSeriesException {
        DynamoDBTestDataGenerator testData = generateTestDataWithSomeFieldsMissing();

        JsonNode requestBody = extractRequestBodyFromEvent(testData.asDynamoDbEvent());

        IndexDocument expected = testData.asIndexDocument();
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);

        assertThat(actual, equalTo(expected));
    }

    private DynamoDBTestDataGenerator generateTestDataWithSingleContributorTDG()
            throws IOException, InvalidIsbnException, InvalidUnconfirmedSeriesException {
        UUID id = generateValidId();
        String contributorIdentifier = "123";
        String contributorName = "Bólsön Kölàdỳ";
        List<Contributor> contributors = Collections.singletonList(
                generateContributor(contributorIdentifier, contributorName, 1));
        String mainTitle = "Moi buki";
        String type = "BookMonograph";
        IndexDate date = new IndexDate("2020", "09", "08");

        return new DynamoDBTestDataGenerator.Builder()
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
                .withAlternativeTitles(SAMPLE_ALTERNATIVETITLES)
                .withTags(SAMPLE_TAGS)
                .withReference(createReference())
                .build();
    }

    private DynamoDBTestDataGenerator generateTestDataWithSomeFieldsMissing()
            throws IOException, InvalidIsbnException, InvalidUnconfirmedSeriesException {
        UUID id = generateValidId();
        String contributorIdentifier = "123";
        String contributorName = "Bólsön Kölàdỳ";
        List<Contributor> contributors = Collections.singletonList(
                generateContributor(contributorIdentifier, contributorName, 1));
        String mainTitle = "Moi buki";
        String type = "BookMonograph";
        IndexDate date = new IndexDate("2020", "09", "08");

        return new DynamoDBTestDataGenerator.Builder()
                .withType(type)
                .withDoi(SAMPLE_DOI)
                .withEventId(EVENT_ID)
                .withStatus(PUBLISHED)
                .withEventName(MODIFY)
                .withId(id)
                .withContributors(contributors)
                .withDate(date)
                .withDescription(DESCRIPTION)
                .withAbstract(ABSTRACT)
                .withPublisher(SAMPLE_PUBLISHER)
                .withModifiedDate(SAMPLE_MODIFIED_DATE)
                .withPublishedDate(SAMPLE_PUBLISHED_DATE)
                .withReference(createReference())
                .build();
    }


    private Contributor generateContributor(String identifier, String name, int sequence) {
        return new Contributor(sequence, name, identifier, URI.create(EXAMPLE_ARP_URI_BASE + identifier));
    }

    private UUID generateValidId() {
        return UUID.randomUUID();
    }

    private JsonNode extractRequestBodyFromEvent(DynamodbEvent requestEvent) {
        IndexDocument indexDocument = IndexDocumentGenerator
                .fromStreamRecord(requestEvent.getRecords().get(0))
                .toIndexDocument();
        return mapper.valueToTree(indexDocument);
    }

}
