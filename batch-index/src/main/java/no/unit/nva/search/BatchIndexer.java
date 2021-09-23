package no.unit.nva.search;

import static no.unit.nva.search.BatchIndexingConstants.NUMBER_OF_FILES_PER_EVENT;
import static nva.commons.core.attempt.Try.attempt;
import com.fasterxml.jackson.databind.JsonNode;
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
import no.unit.nva.s3.ListingResult;
import no.unit.nva.s3.S3Driver;
import no.unit.nva.search.exception.SearchException;
import nva.commons.core.JsonUtils;
import nva.commons.core.attempt.Try;
import nva.commons.core.exceptions.ExceptionUtils;
import nva.commons.core.paths.UnixPath;
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

    public IndexingResult processRequest() {

        ListingResult listFilesResult = fetchNextPageOfFilenames();
        List<String> failedResults = indexFileContents(listFilesResult);
        return new IndexingResult(failedResults, listFilesResult.getListingStartingPoint(),
                                  listFilesResult.isTruncated());
    }

    private ListingResult fetchNextPageOfFilenames() {
        return s3Driver.listFiles(UnixPath.of(importDataRequest.getS3Path()),
                                  importDataRequest.getStartMarker(),
                                  NUMBER_OF_FILES_PER_EVENT);
    }

    private List<String> indexFileContents(ListingResult listFilesResult) {
        return listFilesResult.getFiles()
            .stream()
            .map(this::insertPublishedPublicationsToIndex)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    private List<String> insertPublishedPublicationsToIndex(UnixPath file) {
        logger.info("Indexing file:"+file.toString());
        Stream<JsonNode> fileContents = fetchFileContents(file);
        Stream<Publication> publishedPublications = keepOnlyPublishedPublications(fileContents);
        Stream<Try<SortableIdentifier>> indexActions = insertToIndex(publishedPublications);
        List<String> failures = collectFailures(indexActions).collect(Collectors.toList());
        failures.forEach(this::logFailure);
        return failures;
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
            .map(s3Driver::getFile)
            .map(ionReader::extractJsonNodesFromIonContent)
            .stream()
            .flatMap(flattenStream -> flattenStream)
            .map(jsonNode -> jsonNode.get(DYNAMO_ROOT_ITEM));
    }

    private SortableIdentifier indexDocument(IndexDocument doc) throws SearchException {
        elasticSearchRestClient.addDocumentToIndex(doc);
        return doc.getId();
    }
}
