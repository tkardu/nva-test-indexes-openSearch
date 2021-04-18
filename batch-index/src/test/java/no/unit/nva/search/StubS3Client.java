package no.unit.nva.search;

import static nva.commons.core.attempt.Try.attempt;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import nva.commons.core.ioutils.IoUtils;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

public class StubS3Client implements S3Client {

    public static final String INVALID_INPUT_ERROR = "Input path does not match with supplied test filenames";
    public static final String LINE_SEPARATOR = System.lineSeparator();
    private final List<String> suppliedFilenames;

    public StubS3Client(String... filesInBucket) {
        suppliedFilenames = Arrays.asList(filesInBucket);
    }

    public List<String> listFiles(Path path) {
        return suppliedFilenames;
    }

    public String getFile(String path) {
        if (suppliedFilenames.contains(path)) {
            var inputStream = attempt(() -> new GZIPInputStream(IoUtils.inputStreamFromResources(path)))
                                  .orElseThrow();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            return reader.lines().collect(Collectors.joining(LINE_SEPARATOR));
        } else {
            throw NoSuchKeyException.builder().message(INVALID_INPUT_ERROR).build();
        }
    }

    @Override
    public <ReturnT> ReturnT getObject(GetObjectRequest getObjectRequest,
                                       ResponseTransformer<GetObjectResponse, ReturnT> responseTransformer) {
        String filename = getObjectRequest.key();
        InputStream inputStream = IoUtils.inputStreamFromResources(filename);
        byte[] contentBytes = attempt(() -> IoUtils.inputStreamFromResources(filename).readAllBytes()).orElseThrow();
        GetObjectResponse response = GetObjectResponse.builder().contentLength((long) contentBytes.length).build();
        return attempt(() -> responseTransformer.transform(response, AbortableInputStream.create(inputStream)))
                   .orElseThrow();
    }

    @Override
    public ListObjectsResponse listObjects(ListObjectsRequest listObjectsRequest)
        throws NoSuchBucketException, AwsServiceException, SdkClientException {
        List<S3Object> files = suppliedFilenames.stream()
                                   .map(filename -> S3Object.builder().key(filename).build())
                                   .collect(Collectors.toList());

        return ListObjectsResponse.builder().contents(files).isTruncated(false).build();
    }

    @Override
    public String serviceName() {
        return null;
    }

    @Override
    public void close() {

    }
}
