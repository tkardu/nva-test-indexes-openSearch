package no.unit.nva.search;

import static nva.commons.core.attempt.Try.attempt;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import no.unit.nva.s3.S3Driver;
import nva.commons.core.ioutils.IoUtils;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public class Stub3Driver extends S3Driver {

    public static final String INVALID_INPUT_ERROR = "Input path does not match with supplied test filenames";
    public static final String LINE_SEPARATOR = System.lineSeparator();
    private final List<String> suppliedFilenames;

    public Stub3Driver(String bucketName, String... filesInBucket) {
        super(null, bucketName);
        suppliedFilenames = Arrays.asList(filesInBucket);
    }

    @Override
    public List<String> listFiles(Path path) {
        return suppliedFilenames;
    }

    @Override
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
}
