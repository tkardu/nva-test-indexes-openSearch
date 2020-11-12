package no.unit.nva.dynamodb;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DynamoDBExportFileReaderTest {

    private static final String SAMPLE_BUCKET_NAME = "nva-datapipeline";
    private static final String SAMPLE_S3FOLDER_KEY = "2020-10-12-06-55-32";


    @Test
    void readFilesFromS3Folder() throws IOException {

        ElasticSearchHighLevelRestClient mockElasticSearchClient = mock(ElasticSearchHighLevelRestClient.class);
        AmazonS3 s3Client = mock(AmazonS3.class);
        ListObjectsV2Result listing = mock(ListObjectsV2Result.class);
        when(s3Client.listObjectsV2(anyString(),anyString())).thenReturn(listing);

        S3ObjectSummary objectSummary = mock(S3ObjectSummary.class);

        when(listing.getObjectSummaries()).thenReturn(List.of(objectSummary));

        DynamoDBExportFileReader dynamoDBExportFileReader =
                new DynamoDBExportFileReader(mockElasticSearchClient, s3Client);

        ImportDataRequest importDataRequest = new ImportDataRequest.Builder()
                .withS3Bucket(SAMPLE_BUCKET_NAME)
                .withS3FolderKey(SAMPLE_S3FOLDER_KEY)
                .build();

        dynamoDBExportFileReader.scanS3Folder(importDataRequest);
    }



}
