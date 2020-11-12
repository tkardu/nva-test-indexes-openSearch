package no.unit.nva.dynamodb;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import no.unit.nva.search.exception.SearchException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DynamoDBExportFileReaderTest {

    private static final String SAMPLE_BUCKET_NAME = "nva-datapipeline";
    private static final String SAMPLE_S3FOLDER_KEY = "2020-10-12-06-55-32";


    @Test
    void readFilesFromS3Folder() throws IOException, SearchException {

        ElasticSearchHighLevelRestClient mockElasticSearchClient = mock(ElasticSearchHighLevelRestClient.class);
        AmazonS3 s3Client;
        s3Client = mock(AmazonS3.class);
        ListObjectsV2Result listing = mock(ListObjectsV2Result.class);
        when(s3Client.listObjectsV2(anyString(),anyString())).thenReturn(listing);

        DynamoDBExportFileReader dynamoDBExportFileReader =
                new DynamoDBExportFileReader(mockElasticSearchClient, s3Client);

        ImportDataRequest importDataRequest = new ImportDataRequest(SAMPLE_BUCKET_NAME, SAMPLE_S3FOLDER_KEY);
        dynamoDBExportFileReader.scanS3Folder(importDataRequest);
    }



}
