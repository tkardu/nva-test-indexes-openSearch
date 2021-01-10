package no.unit.nva.publication;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.model.Publication;
import no.unit.nva.utils.DynamodbStreamRecordPublicationMapper;
import no.unit.nva.utils.ImportDataRequest;
import nva.commons.utils.JacocoGenerated;
import nva.commons.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class DataPipelineFileReaderPublication {

    public static final String ERROR_PROCESSING_DOCUMENT = "Error processing document";
    public static final String MANIFEST = "manifest";
    public static final String TOTAL_RECORDS_PROCESSED_IN_IMPORT_MESSAGE =
            "Total number of records processed in this import is {}";
    public static final String NUMBER_OF_IMPORTED_RECORDS_IN_THIS_FILE_MESSAGE =
            "Number of imported records in this file={}";
    public static final String READING_FROM_S3_MESSAGE = "Reading from s3://{}/{}";

    private static final Logger logger = LoggerFactory.getLogger(DataPipelineFileReaderPublication.class);
    private static final ObjectMapper mapper = JsonUtils.objectMapper;
    private final AmazonS3 s3Client;

    public DataPipelineFileReaderPublication(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    /**
     * Reads one textfile with json dynamodb streamrecords one line at a time.
     *
     * @param s3Object BufferedReader containing json dynamodb records
     */
    @JacocoGenerated
    public Long readJsonDataFile(S3Object s3Object) {
        long processedPublicationCount = 0;

        logger.debug(READING_FROM_S3_MESSAGE, s3Object.getBucketName(), s3Object.getKey());
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {
            processedPublicationCount = bufferedReader
                    .lines()
                    .map(this::processInput)
                    .filter(Optional::isPresent)
                    .map(doc -> transformPublication(doc.get()))
                    .map(this::persistPublication)
                    .count();
            logger.info(NUMBER_OF_IMPORTED_RECORDS_IN_THIS_FILE_MESSAGE, processedPublicationCount);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return processedPublicationCount;
    }

    /**
     * Scans an S3 bucket with given key (folder) for files containing dynamodb json data files.
     *
     * @param importDataRequest Containing bucket and key for S3
     */
    public void scanS3Folder(ImportDataRequest importDataRequest) {

        AtomicLong counter = new AtomicLong(0L);

        ListObjectsV2Result listing =
                s3Client.listObjectsV2(importDataRequest.getS3bucket(), importDataRequest.getS3folderkey());

        listing.getObjectSummaries().stream()
                .filter(this::isDataFile)
                .map(this::getS3Object)
                .forEach(s3o -> counter.getAndAdd(readJsonDataFile(s3o)));

        logger.info(TOTAL_RECORDS_PROCESSED_IN_IMPORT_MESSAGE, counter.get());
    }

    protected S3Object getS3Object(S3ObjectSummary s3ObjectSummary) {
        return s3Client.getObject(new GetObjectRequest(s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey()));
    }

    protected boolean isDataFile(S3ObjectSummary objectSummary) {
        return objectSummary.getSize() > 0 && !objectSummary.getKey().contains(MANIFEST);
    }

    @JacocoGenerated
    private Optional<Publication> processInput(String publicationJsonSource) {
        try {
            return Optional.ofNullable(DynamodbStreamRecordPublicationMapper.dynamodbSerializedRecordStringToPublication(publicationJsonSource));
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
            return Optional.empty();
        }
    }

    private Publication transformPublication(Publication publication) {
        return publication;
    }


    private int persistPublication(Publication publication) {
        try {
            logger.debug("Handling publication: {}", publication);
            return 1;
        } catch (Exception e) {
            logger.error(ERROR_PROCESSING_DOCUMENT, e);
            return 0;
        }
    }


}
