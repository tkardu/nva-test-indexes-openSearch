package no.unit.nva.dynamodb;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import no.unit.nva.search.IndexDocument;
import no.unit.nva.search.exception.SearchException;
import nva.commons.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.isNull;
import static no.unit.nva.dynamodb.DynamoDBStreamHandler.PUBLISHED;
import static no.unit.nva.dynamodb.DynamoDBStreamHandler.STATUS;

public class DynamoDBExportFileReader {

    public static final String ERROR_ADDING_DOCUMENT_SEARCH_INDEX = "Error adding document with id={} to searchIndex";
    public static final String MANIFEST = "manifest";
    public static final String TOTAL_RECORDS_PROCESSED_IN_IMPORT_MESSAGE =
            "Total number of records processed in this import is {}";
    public static final String NUMBER_OF_IMPORTED_RECORDS_IN_THIS_FILE_MESSAGE =
            "Number of imported records in this file={}";
    private static final Logger logger = LoggerFactory.getLogger(IndexDocumentGenerator.class);
    private static final ObjectMapper mapper = JsonUtils.objectMapper;
    private final ElasticSearchHighLevelRestClient elasticSearchRestClient;
    private final AmazonS3 s3Client;


    public DynamoDBExportFileReader(ElasticSearchHighLevelRestClient elasticSearchRestClient, AmazonS3 s3Client) {
        this.elasticSearchRestClient = elasticSearchRestClient;
        this.s3Client = s3Client;
    }

    private static boolean isPublished(JsonNode jsonNode) {
        var statusNode = jsonNode.get(STATUS);
        return !isNull(statusNode)
                && statusNode.toString().toLowerCase(Locale.US).contains(PUBLISHED.toLowerCase(Locale.US));
    }

    /**
     * Reads one textfile with json dynamodb streamrecords one line at a time.
     *
     * @param s3Object BufferedReader containing json dynamodb records
     */
    public Long readJsonDataFile(S3Object s3Object) {
        long indexedDocumentCount = 0;

        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {
            indexedDocumentCount = bufferedReader
                    .lines()
                    .map(this::fromJsonString)
                    .filter(Optional::isPresent)
                    .map(doc -> addDocumentToIndex(doc.get()))
                    .count();
            logger.info(NUMBER_OF_IMPORTED_RECORDS_IN_THIS_FILE_MESSAGE, indexedDocumentCount);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return indexedDocumentCount;
    }

    /**
     * Scans an S3 bucket with given key (folder) for files containing dynamodb json data files.
     *
     * @param importDataRequest Containing bucket and key for S3
     */
    public void scanS3Folder(ImportDataRequest importDataRequest) {

        AtomicLong counter = new AtomicLong(0L);
        ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request()
                .withBucketName(importDataRequest.getS3bucket())
                .withPrefix(importDataRequest.getS3folderkey());

        getSummaries(listObjectsV2Request)
                .stream()
                .filter(this::isDataFile)
                .map(this::getS3Object)
                .map(this::readJsonDataFile)
                .map(counter::getAndAdd);

        logger.info(TOTAL_RECORDS_PROCESSED_IN_IMPORT_MESSAGE, counter.get());
    }

    private List<S3ObjectSummary> getSummaries(ListObjectsV2Request request) {
        return s3Client.listObjectsV2(request).getObjectSummaries();
    }

    protected S3Object getS3Object(S3ObjectSummary s3ObjectSummary) {
        return s3Client.getObject(new GetObjectRequest(s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey()));

    }

    protected boolean isDataFile(S3ObjectSummary objectSummary) {
        return objectSummary.getSize() > 0 && !objectSummary.getKey().contains(MANIFEST);
    }

    private Optional<IndexDocument> fromJsonString(String line) {
        try {
            JsonNode node = mapper.readTree(line);
            if (isPublished(node)) {
                var indexDocument = IndexDocumentGenerator.fromJsonNode(node);
                return Optional.of(indexDocument);
            }
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
        }
        return Optional.empty();
    }

    private int addDocumentToIndex(IndexDocument document) {
        try {
            elasticSearchRestClient.addDocumentToIndex(document);
            return 1;
        } catch (SearchException e) {
            logger.error(ERROR_ADDING_DOCUMENT_SEARCH_INDEX, document.getId(), e);
            return 0;
        }
    }
}
