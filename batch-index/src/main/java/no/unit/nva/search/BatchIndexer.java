package no.unit.nva.search;

import static nva.commons.core.attempt.Try.attempt;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Optional;
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
import nva.commons.core.JsonUtils;
import nva.commons.core.attempt.Try;
import nva.commons.core.exceptions.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

public class BatchIndexer {

    public static final String DYNAMO_ROOT = "Item";
    public static final String DYNAMO_ROOT_ITEM = DYNAMO_ROOT;
    private static final Logger logger = LoggerFactory.getLogger(BatchIndexer.class);
    private final ImportDataRequest importDataRequest;
    private final S3Driver s3Driver;
    private final S3IonReader ionReader;
    private final ElasticSearchHighLevelRestClient elasticSearchRestClient;

    public BatchIndexer(ImportDataRequest importDataRequest,
                        S3Client s3Client,
                        ElasticSearchHighLevelRestClient elasticSearchRestClient) {
        this.importDataRequest = importDataRequest;
        this.elasticSearchRestClient = elasticSearchRestClient;
        this.s3Driver = new S3Driver(s3Client, importDataRequest.getBucket());
        this.ionReader = new S3IonReader();
    }

    public List<String> processRequest() {

        List<Publication> publishedPublications = fetchPublishedPublicationsFromDynamoDbExportInS3(importDataRequest)
            .collect(Collectors.toList());
        logger.info("Number of published publications:" + publishedPublications.size());

        List<Try<SortableIdentifier>> indexActions = insertToIndex(publishedPublications.stream())
            .collect(Collectors.toList());
        long successCount = indexActions.stream().filter(Try::isSuccess).count();
        logger.info("Number of successful indexing actions:" + successCount);

        long failureCount = indexActions.stream().filter(Try::isFailure).count();
        logger.info("Number of failed indexing actions:" + failureCount);

        List<String> failures = collectFailures(indexActions.stream());
        failures.forEach(this::logFailure);
        return failures;
    }

    private Stream<Publication> fetchPublishedPublicationsFromDynamoDbExportInS3(ImportDataRequest request) {
        List<UnixPath> allFiles = s3Driver.listFiles(UnixPath.of(request.getS3Path()));
        List<JsonNode> allContent = fetchAllContentFromDataExport(allFiles);
        logger.info("Number of jsonNodes:" + allContent.size());
        return keepOnlyPublishedPublications(allContent);
    }

    private Stream<Try<SortableIdentifier>> insertToIndex(Stream<Publication> publishedPublications) {
        return publishedPublications
            .map(IndexDocument::fromPublication)
            .map(attempt(this::indexDocument));
    }

    private void logFailure(String failureMessage) {
        logger.warn("Failed to index resource:" + failureMessage);
    }

    private List<String> collectFailures(Stream<Try<SortableIdentifier>> indexActions) {
        return indexActions
            .filter(Try::isFailure)
            .map(fail -> ExceptionUtils.stackTraceInSingleLine(fail.getException()))
            .collect(Collectors.toList());
    }

    private SortableIdentifier indexDocument(IndexDocument doc) throws SearchException {
        elasticSearchRestClient.addDocumentToIndex(doc);
        return doc.getId();
    }

    private Stream<Publication> keepOnlyPublishedPublications(List<JsonNode> allContent) {
        Stream<DynamoEntry> dynamoEntries = allContent.stream().map(this::toDynamoEntry);
        Stream<Publication> allPublications = dynamoEntries
            .filter(entry -> entry instanceof ResourceDao)
            .map(dao -> (ResourceDao) dao)
            .map(ResourceDao::getData)
            .map(Resource::toPublication);
        return allPublications
            .filter(publication -> PublicationStatus.PUBLISHED.equals(publication.getStatus()));
    }

    private DynamoEntry toDynamoEntry(JsonNode jsonNode) {
        return JsonUtils.objectMapperNoEmpty.convertValue(jsonNode, DynamoEntry.class);
    }

    private List<JsonNode> fetchAllContentFromDataExport(List<UnixPath> allFiles) {
        return allFiles.stream()
            .map(s3Driver::getFile)
            .map(attempt(ionReader::extractJsonNodesFromIonContent))
            .map(Try::toOptional)
            .flatMap(Optional::stream)
            .flatMap(streamToFlatten -> streamToFlatten)
            .map(jsonNode -> jsonNode.get(DYNAMO_ROOT_ITEM))
            .collect(Collectors.toList());
    }
}
