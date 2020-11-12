package no.unit.nva.dynamodb;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
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
import java.util.Locale;
import java.util.Optional;

import static java.util.Objects.isNull;
import static no.unit.nva.dynamodb.DynamoDBStreamHandler.PUBLISHED;
import static no.unit.nva.dynamodb.DynamoDBStreamHandler.STATUS;

public class DynamoDBExportFileReader {

    private static final Logger logger = LoggerFactory.getLogger(IndexDocumentGenerator.class);

    public static final String ERROR_ADDING_DOCUMENT_SEARCH_INDEX = "Error adding document with id={} to searchIndex";
    public static final String MANIFEST = "manifest";

    private int indexedDocumentCount;
    private static final ObjectMapper mapper = JsonUtils.objectMapper;
    private final ElasticSearchHighLevelRestClient elasticSearchRestClient;
    private final AmazonS3 s3Client;


    public DynamoDBExportFileReader(ElasticSearchHighLevelRestClient elasticSearchRestClient, AmazonS3 s3Client) {
        this.elasticSearchRestClient = elasticSearchRestClient;
        this.s3Client = s3Client;
    }

    private static boolean isPublished(JsonNode jsonNode) {
        JsonNode statusNode = jsonNode.get(STATUS);
        if (isNull(statusNode)) {
            return false;
        }
        return statusNode.toString().toLowerCase(Locale.US).contains(PUBLISHED.toLowerCase(Locale.US));
    }

    /**
     * Reads one textfile with json dynamodb streamrecords one line at a time.
     *
     * @param reader BufferedReader containing json dynamodb records
     */
    public void readJsonDataFile(BufferedReader reader) {
        reader.lines()
                .map(this::fromJsonString)
                .filter(Optional::isPresent)
                .forEach(doc -> addDocumentToIndex(doc.get()));

        logger.info("processed #indexedDocumentCount={}", indexedDocumentCount);
    }

    /**
     * Scans an S3 bucket with given key (folder) for files containing dynamodb json data files.
     *
     * @param importDataRequest Containing bucket and key for S3
     * @throws IOException something gone wrong
     */
    public void scanS3Folder(ImportDataRequest importDataRequest) throws IOException {

        ListObjectsV2Result listing =
                s3Client.listObjectsV2(importDataRequest.getS3bucket(), importDataRequest.getS3folderkey());

        listing.getObjectSummaries()
                .stream()
                .filter(this::isDataFile)
                .map(summary -> getInputStreamReader(s3Client, summary))
                .forEach(this::readJsonDataFile);

    }

    @SuppressWarnings("PMD.CloseResource")
    private BufferedReader getInputStreamReader(AmazonS3 s3Client, S3ObjectSummary s3ObjectSummary) {
        GetObjectRequest getObjectRequest =
                new GetObjectRequest(s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey());
        S3Object s3Object = s3Client.getObject(getObjectRequest);
        return new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
    }

    private boolean isDataFile(S3ObjectSummary objectSummary) {
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
            logger.error("",e);
        }
        return Optional.empty();
    }

    private void addDocumentToIndex(IndexDocument document) {
        try {
            elasticSearchRestClient.addDocumentToIndex(document);
            indexedDocumentCount++;
        } catch (SearchException e) {
            logger.error(ERROR_ADDING_DOCUMENT_SEARCH_INDEX, document.getId(), e);
        }
    }
}
