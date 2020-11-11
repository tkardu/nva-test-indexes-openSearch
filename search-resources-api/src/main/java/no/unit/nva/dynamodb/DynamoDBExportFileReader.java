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

    private static final String AWS_REGION = "eu-west-1";
    private static final Logger logger = LoggerFactory.getLogger(IndexDocumentGenerator.class);
    private static final ObjectMapper mapper = JsonUtils.objectMapper;
    public static final String ERROR_ADDING_DOCUMENT_SEARCH_INDEX = "Error adding document with id={} to searchIndex";
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
     * @param inputStreamReader inputstream containing json dynamodb records
     * @throws IOException thrown when something goes wrong
     */
    public void readFile(InputStreamReader inputStreamReader) throws IOException {
        BufferedReader reader = new BufferedReader(inputStreamReader);
        reader.lines()
            .map(this::fromJsonString)
            .filter(Optional::isPresent)
            .forEach(doc  -> addDocumentToIndex(doc.get()));

        logger.info("processed #indexedDocumentCount={}", indexedDocumentCount);
    }

    /**
     * Scans an S3 bucket with given key (folder) for files containing dynamodb json data files.
     * @param bucketName name of the S3 bucket
     * @param folder key (directory/folder) in the bucket containing datafiles
     * @throws IOException something gone wrong
     */
    public void scanS3Folder(String bucketName, String folder) throws IOException {

        final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(AWS_REGION)
                .build();

        Predicate<S3ObjectSummary> justDataFiles = (s) -> s.getSize() > 0 && !s.getKey().contains("manifest");

        ObjectListing listing = s3Client.listObjects(bucketName, folder);

        for (S3ObjectSummary s3ObjectSummary : listing.getObjectSummaries()) {
            if (justDataFiles.test(s3ObjectSummary)) {
                InputStreamReader inputStream = null;
                try {
                    inputStream = getInputStreamReader(s3Client, s3ObjectSummary);
                    readFile(inputStream);
                } finally {
                    inputStream.close();
                }
            }
        }
    }

    private InputStreamReader getInputStreamReader(AmazonS3 s3Client, S3ObjectSummary s3ObjectSummary) {
        GetObjectRequest getObjectRequest =
                new GetObjectRequest(s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey());
        try (S3Object s3Object = s3Client.getObject(getObjectRequest)) {
            InputStreamReader inputStream = new InputStreamReader(s3Object.getObjectContent());
            return inputStream;
        } catch (IOException e) {
            logger.error("",e);
        }
        return null;
    }

    private Optional<IndexDocument> fromJsonString(String line) {
        JsonNode node = null;
        try {
            node = mapper.readTree(line);
            if (isPublished(node)) {
                var indexDocument = IndexDocumentGenerator.fromJsonNode(node);
                return Optional.of(indexDocument);
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    private void addDocumentToIndex(IndexDocument document) {
        try {
            elasticSearchRestClient.addDocumentToIndex(document);
            indexedDocumentCount++;
        } catch (SearchException e) {
            logger.error(ERROR_ADDING_DOCUMENT_SEARCH_INDEX,document.getId(),e);
        }
    }
}
