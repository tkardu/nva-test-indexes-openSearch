package no.unit.nva.search;

import static nva.commons.core.attempt.Try.attempt;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
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
import software.amazon.awssdk.services.s3.model.S3Object;

public class StubS3Client implements S3Client {

    private final List<String> suppliedFilenames;

    public StubS3Client(String... filesInBucket) {
        suppliedFilenames = Arrays.asList(filesInBucket);
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
        throws AwsServiceException, SdkClientException {
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
