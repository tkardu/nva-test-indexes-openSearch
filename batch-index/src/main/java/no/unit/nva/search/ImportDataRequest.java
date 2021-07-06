package no.unit.nva.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.JsonSerializable;

public class ImportDataRequest implements JsonSerializable {

    public static final String S3_LOCATION_FIELD = "s3Location";
    public static final String PATH_DELIMITER = "/";
    @JsonProperty(S3_LOCATION_FIELD)
    private final URI s3Location;

    @JsonCreator
    public ImportDataRequest(@JsonProperty(S3_LOCATION_FIELD) String s3Location) {
        this.s3Location = Optional.ofNullable(s3Location).map(URI::create).orElseThrow(this::reportMissingValue);
    }

    public String getS3Location() {
        return s3Location.toString();
    }

    @JsonIgnore
    @JacocoGenerated
    public String getBucket() {
        return s3Location.getHost();
    }

    @JsonIgnore
    @JacocoGenerated
    public String getS3Path() {
        return Optional.ofNullable(s3Location)
                   .map(URI::getPath)
                   .map(this::removeRoot)
                   .orElseThrow();
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(getS3Location());
    }

    @JacocoGenerated
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ImportDataRequest)) {
            return false;
        }
        ImportDataRequest that = (ImportDataRequest) o;
        return Objects.equals(getS3Location(), that.getS3Location());
    }

    private IllegalArgumentException reportMissingValue() {
        return new IllegalArgumentException("Missing input:" + S3_LOCATION_FIELD);
    }

    private String removeRoot(String path) {
        return path.startsWith(PATH_DELIMITER) ? path.substring(1) : path;
    }
}
