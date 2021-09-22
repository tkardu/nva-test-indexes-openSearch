package no.unit.nva.search;

import static nva.commons.core.attempt.Try.attempt;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.model.Publication;
import no.unit.nva.model.PublicationStatus;
import no.unit.nva.publication.s3imports.S3IonReader;
import no.unit.nva.publication.storage.model.Resource;
import no.unit.nva.publication.storage.model.daos.DynamoEntry;
import no.unit.nva.publication.storage.model.daos.ResourceDao;
import no.unit.nva.s3.S3Driver;
import no.unit.nva.s3.UnixPath;
import no.unit.nva.search.exception.SearchException;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.JsonUtils;
import nva.commons.core.attempt.Try;
import nva.commons.core.exceptions.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class ImportToSearchIndexHandler implements RequestStreamHandler {

    public static final String AWS_REGION_ENV_VARIABLE = "AWS_REGION";
    public static final String DYNAMO_ROOT = "Item";
    public static final String DYNAMO_ROOT_ITEM = DYNAMO_ROOT;
    private static final Logger logger = LoggerFactory.getLogger(ImportToSearchIndexHandler.class);
    private final ElasticSearchHighLevelRestClient elasticSearchRestClient;
    private final S3Client s3Client;
    private S3Driver s3Driver;
    private S3IonReader ionReader;

    @JacocoGenerated
    public ImportToSearchIndexHandler() {
        this(new Environment());
    }

    @JacocoGenerated
    public ImportToSearchIndexHandler(Environment environment) {
        this(defaultS3Client(environment), defaultEsClient());
    }

    public ImportToSearchIndexHandler(S3Client s3Client,
                                      ElasticSearchHighLevelRestClient elasticSearchRestClient) {
        this.s3Client = s3Client;
        this.elasticSearchRestClient = elasticSearchRestClient;
    }

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        ImportDataRequest request = parseInput(input);
        setupS3Access(request.getBucket());

        List<UnixPath> allFiles = s3Driver.listFiles(UnixPath.of(request.getS3Path()));
        List<String> failures = allFiles.stream()
            .map(this::insertPublishedPublicationsToIndex)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        writeOutput(output, failures);
    }

    private List<String> insertPublishedPublicationsToIndex(UnixPath file) {
        Stream<JsonNode> fileContents = fetchFileContents(file);
        Stream<Publication> publishedPublications = keepOnlyPublishedPublications(fileContents);
        Stream<Try<SortableIdentifier>> indexActions = insertToIndex(publishedPublications);
        List<String> failures = collectFailures(indexActions).collect(Collectors.toList());
        failures.forEach(this::logFailure);
        return failures;
    }

    protected void writeOutput(OutputStream outputStream, List<String> failures)
        throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
            String outputJson = JsonUtils.objectMapperWithEmpty.writeValueAsString(failures);
            writer.write(outputJson);
        }
    }

    @JacocoGenerated
    private static ElasticSearchHighLevelRestClient defaultEsClient() {
        return new ElasticSearchHighLevelRestClient();
    }

    @JacocoGenerated
    private static S3Client defaultS3Client(Environment environment) {
        String awsRegion = environment.readEnvOpt(AWS_REGION_ENV_VARIABLE).orElse(Regions.EU_WEST_1.getName());
        return S3Client.builder()
            .region(Region.of(awsRegion))
            .httpClient(UrlConnectionHttpClient.builder().build())
            .build();
    }

    private ImportDataRequest parseInput(InputStream input) throws IOException {
        ImportDataRequest request = JsonUtils.objectMapper.readValue(input, ImportDataRequest.class);
        logger.info("Bucket: " + request.getBucket());
        logger.info("Path: " + request.getS3Path());
        return request;
    }

    private void setupS3Access(String bucketName) {
        s3Driver = new S3Driver(s3Client, bucketName);
        ionReader = new S3IonReader();
    }

    private void logFailure(String failureMessage) {
        logger.warn("Failed to index resource:" + failureMessage);
    }

    private Stream<String> collectFailures(Stream<Try<SortableIdentifier>> indexActions) {
        return indexActions
            .filter(Try::isFailure)
            .map(f -> ExceptionUtils.stackTraceInSingleLine(f.getException()));
    }

    private Stream<Try<SortableIdentifier>> insertToIndex(Stream<Publication> publishedPublications) {
        return publishedPublications
            .map(IndexDocument::fromPublication)
            .map(attempt(this::indexDocument));
    }

    private SortableIdentifier indexDocument(IndexDocument doc) throws SearchException {
        elasticSearchRestClient.addDocumentToIndex(doc);
        return doc.getId();
    }

    private Stream<Publication> keepOnlyPublishedPublications(Stream<JsonNode> allContent) {
        return allContent
            .map(this::toDynamoEntry)
            .filter(entry -> entry instanceof ResourceDao)
            .map(dao -> (ResourceDao) dao)
            .map(ResourceDao::getData)
            .map(Resource::toPublication)
            .filter(publication -> PublicationStatus.PUBLISHED.equals(publication.getStatus()));
    }

    private DynamoEntry toDynamoEntry(JsonNode jsonNode) {
        return JsonUtils.objectMapperNoEmpty.convertValue(jsonNode, DynamoEntry.class);
    }

    private Stream<JsonNode> fetchFileContents(UnixPath file) {
        return Try.of(file)
            .map(f -> s3Driver.getFile(f))
            .map(content -> ionReader.extractJsonNodesFromIonContent(content))
            .stream()
            .flatMap(flattenStream -> flattenStream)
            .map(jsonNode -> jsonNode.get(DYNAMO_ROOT_ITEM));
    }

}
