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
import java.util.Optional;
import java.util.function.Predicate;

import static java.util.Objects.isNull;
import static no.unit.nva.dynamodb.DynamoDBStreamHandler.PUBLISHED;
import static no.unit.nva.dynamodb.DynamoDBStreamHandler.STATUS;

public class DynamoDBExportFileReader {

    private static final String AWS_REGION = "eu-west-1";
    private static final Logger logger = LoggerFactory.getLogger(IndexDocumentGenerator.class);
    private static final ObjectMapper mapper = JsonUtils.objectMapper;
    public static final String ERROR_ADDING_DOCUMENT_WITH_ID_TO_SEARCH_INDEX = "Error adding document with id={} to searchIndex ";
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
        return statusNode.toString().toLowerCase().contains(PUBLISHED.toLowerCase());
    }

    public void readFile(InputStreamReader inputStreamReader) throws SearchException, IOException {
        BufferedReader reader = new BufferedReader(inputStreamReader);
        reader.lines()
            .map(this::fromJsonString)
            .filter(Optional::isPresent)
            .forEach(doc  -> addDocumentToIndex(doc.get()))            ;

            logger.info("processed #indexedDocumentCount={}", indexedDocumentCount);
    }

    public void scanS3Folder(String bucketName, String folder) throws SearchException, IOException {

        final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(AWS_REGION)
                .build();

        Predicate<S3ObjectSummary> justDataFiles = (s) -> s.getSize() > 0 && !s.getKey().contains("manifest");

        ObjectListing listing = s3Client.listObjects(bucketName, folder);

        for (S3ObjectSummary s3ObjectSummary : listing.getObjectSummaries()) {
            if (justDataFiles.test(s3ObjectSummary)) {
                GetObjectRequest getObjectRequest = new GetObjectRequest(s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey());
                S3Object object = s3Client.getObject(getObjectRequest);
                InputStreamReader inputStream = new InputStreamReader(object.getObjectContent());
                readFile(inputStream);
            }
        }
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
            logger.error(ERROR_ADDING_DOCUMENT_WITH_ID_TO_SEARCH_INDEX,document.getId(),e);
        }
    }
}
