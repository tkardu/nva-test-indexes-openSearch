package no.unit.nva.dynamodb;

import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import no.unit.nva.search.exception.SearchException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class DynamoDBExportFileReaderTest {

    @Test
    void readFilesFromS3Folder() throws IOException, SearchException {

        ElasticSearchHighLevelRestClient mockElasticSearchClient = mock(ElasticSearchHighLevelRestClient.class);
        DynamoDBExportFileReader dynamoDBExportFileReader = new DynamoDBExportFileReader(mockElasticSearchClient);

        String bucketName = "nva-datapipeline";
        String folder = "2020-10-12-06-55-32";

        //  dynamoDBExportFileReader.scanS3Folder(bucketName, folder);
    }



}
