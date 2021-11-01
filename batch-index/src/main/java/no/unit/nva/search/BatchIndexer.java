package no.unit.nva.search;

import static no.unit.nva.search.BatchIndexingConstants.NUMBER_OF_FILES_PER_EVENT;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperNoEmpty;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.model.Publication;
import no.unit.nva.model.PublicationStatus;
import no.unit.nva.publication.storage.model.Resource;
import no.unit.nva.publication.storage.model.daos.DynamoEntry;
import no.unit.nva.publication.storage.model.daos.ResourceDao;
import no.unit.nva.s3.ListingResult;
import no.unit.nva.s3.S3Driver;
import nva.commons.core.attempt.Try;
import nva.commons.core.paths.UnixPath;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

public class BatchIndexer implements IndexingResult<SortableIdentifier> {

    public static final String DYNAMO_ROOT = "Item";
    public static final String DYNAMO_ROOT_ITEM = DYNAMO_ROOT;

    private static final Logger logger = LoggerFactory.getLogger(BatchIndexer.class);
    private final ImportDataRequest importDataRequest;
    private final S3Driver s3Driver;
    private final ElasticSearchHighLevelRestClient elasticSearchRestClient;
    private IndexingResultRecord<SortableIdentifier> processingResult;

    public BatchIndexer(ImportDataRequest importDataRequest,
                        S3Client s3Client,
                        ElasticSearchHighLevelRestClient elasticSearchRestClient) {
        this.importDataRequest = importDataRequest;
        this.elasticSearchRestClient = elasticSearchRestClient;
        this.s3Driver = new S3Driver(s3Client, importDataRequest.getBucket());
    }

    public IndexingResult<SortableIdentifier> processRequest() {
        ListingResult listFilesResult = fetchNextPageOfFilenames();
        List<SortableIdentifier> failedResults = indexFileContents(listFilesResult);
        this.processingResult = new IndexingResultRecord<>(
            failedResults,
            listFilesResult.getListingStartingPoint(),
            listFilesResult.isTruncated()
        );

        return this;
    }

    @Override
    public List<SortableIdentifier> getFailedResults() {
        return this.processingResult.getFailedResults();
    }

    @Override
    public String getNextStartMarker() {
        return processingResult.getNextStartMarker();
    }

    @Override
    public boolean isTruncated() {
        return this.processingResult.isTruncated();
    }

    private ListingResult fetchNextPageOfFilenames() {
        return s3Driver.listFiles(UnixPath.of(importDataRequest.getS3Path()),
                                  importDataRequest.getStartMarker(),
                                  NUMBER_OF_FILES_PER_EVENT);
    }

    private List<SortableIdentifier> indexFileContents(ListingResult listFilesResult) {
        return listFilesResult.getFiles()
            .stream()
            .map(this::insertPublishedPublicationsToIndex)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    private List<SortableIdentifier> insertPublishedPublicationsToIndex(UnixPath file) {
        logger.info("Indexing file:" + file.toString());
        Stream<JsonNode> fileContents = fetchFileContents(file);
        Stream<Publication> documentsToIndex = keepOnlyPublishedPublications(fileContents);


        Stream<BulkResponse> result = elasticSearchRestClient.batchInsert(documentsToIndex);
        List<SortableIdentifier> failures = collectFailures(result).collect(Collectors.toList());
        failures.forEach(this::logFailure);
        return failures;
    }

    private <T> void logFailure(T failureMessage) {
        logger.warn("Failed to index resource:" + failureMessage.toString());
    }

    private Stream<SortableIdentifier> collectFailures(Stream<BulkResponse> indexActions) {
        return indexActions
            .filter(BulkResponse::hasFailures)
            .map(BulkResponse::getItems)
            .flatMap(Arrays::stream)
            .filter(BulkItemResponse::isFailed)
            .map(BulkItemResponse::getFailure)
            .map(Failure::getId)
            .map(SortableIdentifier::new);
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
        return objectMapperNoEmpty.convertValue(jsonNode, DynamoEntry.class);
    }

    private Stream<JsonNode> fetchFileContents(UnixPath file) {
        return Try.of(file)
            .map(s3Driver::getCompressedFile)
            .map(S3IonReader::extractJsonNodesFromIonContent)
            .stream()
            .flatMap(flattenStream -> flattenStream)
            .map(jsonNode -> jsonNode.get(DYNAMO_ROOT_ITEM));
    }
}
