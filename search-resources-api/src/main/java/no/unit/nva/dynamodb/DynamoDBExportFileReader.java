package no.unit.nva.dynamodb;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
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
import java.util.function.Predicate;

import static java.util.Objects.isNull;
import static no.unit.nva.dynamodb.DynamoDBStreamHandler.PUBLISHED;
import static no.unit.nva.dynamodb.DynamoDBStreamHandler.STATUS;

public class DynamoDBExportFileReader {

    public static final String ERROR_ADDING_DOCUMENT_SEARCH_INDEX = "Error adding document with id={} to searchIndex";
    private static final String AWS_REGION = "eu-west-1";
    private static final Logger logger = LoggerFactory.getLogger(IndexDocumentGenerator.class);
    private static final ObjectMapper mapper = JsonUtils.objectMapper;
    private final ElasticSearchHighLevelRestClient elasticSearchRestClient;
    private int indexedDocumentCount;

    public DynamoDBExportFileReader(ElasticSearchHighLevelRestClient elasticSearchRestClient) {
        this.elasticSearchRestClient = elasticSearchRestClient;
    }

    private static boolean isPublished(JsonNode jsonNode) {
        JsonNode statusNode = jsonNode.get(STATUS);
        if (isNull(statusNode)) {
            return false;
        }
        return statusNode.toString().toLowerCase(Locale.US).contains(PUBLISHED.toLowerCase(Locale.US));
    }

    /**
     * Reads a inputstream of json dynamodb streamrecords one line at a time.
     *
     * @param reader BufferedReader containing json dynamodb records
     * @throws IOException thrown when something goes wrong
     */
    public void readFile(BufferedReader reader) throws IOException {
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

        final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(AWS_REGION)
                .build();

        Predicate<S3ObjectSummary> isDataFile = (s) -> s.getSize() > 0 && !s.getKey().contains("manifest");

        ObjectListing listing = s3Client.listObjects(importDataRequest.getS3bucket(), importDataRequest.getS3key());

        for (S3ObjectSummary s3ObjectSummary : listing.getObjectSummaries()) {
            if (isDataFile.test(s3ObjectSummary)) {
                readFile(getInputStreamReader(s3Client, s3ObjectSummary));
            }
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    private BufferedReader getInputStreamReader(AmazonS3 s3Client, S3ObjectSummary s3ObjectSummary) {
        GetObjectRequest getObjectRequest =
                new GetObjectRequest(s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey());
        S3Object s3Object = s3Client.getObject(getObjectRequest);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
        return bufferedReader;
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
