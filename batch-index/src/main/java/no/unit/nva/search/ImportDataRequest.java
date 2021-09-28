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
    public static final String START_OF_LISTING_INDEX = "startMarker";

    @JsonProperty(S3_LOCATION_FIELD)
    private final URI s3Location;
    @JsonProperty(START_OF_LISTING_INDEX)
    private final String startMarker;

    @JsonCreator
    public ImportDataRequest(@JsonProperty(S3_LOCATION_FIELD) String s3Location,
                             @JsonProperty(START_OF_LISTING_INDEX) String startMarker) {
        this.s3Location = Optional.ofNullable(s3Location).map(URI::create).orElseThrow(this::reportMissingValue);
        this.startMarker = startMarker;
    }

    public ImportDataRequest(String s3Location) {
        this(s3Location, null);
    }

    public String getStartMarker() {
        return startMarker;
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ImportDataRequest)) {
            return false;
        }
        ImportDataRequest that = (ImportDataRequest) o;
        return Objects.equals(getS3Location(), that.getS3Location()) && Objects.equals(getStartMarker(),
                                                                                       that.getStartMarker());
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(getS3Location(), getStartMarker());
    }

    private IllegalArgumentException reportMissingValue() {
        return new IllegalArgumentException("Missing input:" + S3_LOCATION_FIELD);
    }

    private String removeRoot(String path) {
        return path.startsWith(PATH_DELIMITER) ? path.substring(1) : path;
    }
}
