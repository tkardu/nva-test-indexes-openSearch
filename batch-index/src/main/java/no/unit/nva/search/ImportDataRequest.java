package no.unit.nva.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URI;
import java.util.Optional;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.JsonSerializable;

public class ImportDataRequest implements JsonSerializable {

    public static final String S3_LOCATION_FIELD = "s3Location";
    public static final String PATH_DELIMITER = "/";
    public static final String LISTING_STARTING_POINT = "listingStartingPoint";
    @JsonProperty(S3_LOCATION_FIELD)
    private final URI s3Location;
    @JsonProperty(LISTING_STARTING_POINT)
    private final String listingStartingPoint;

    @JsonCreator
    public ImportDataRequest(@JsonProperty(S3_LOCATION_FIELD) String s3Location,
                             @JsonProperty(LISTING_STARTING_POINT) String listingStartingPoint

    ) {
        this.listingStartingPoint = listingStartingPoint;
        this.s3Location = Optional.ofNullable(s3Location).map(URI::create).orElseThrow(this::reportMissingValue);
    }

    public ImportDataRequest(String s3Location) {
        this(s3Location, null);
    }

    public String getListingStartingPoint() {
        return listingStartingPoint;
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

    private IllegalArgumentException reportMissingValue() {
        return new IllegalArgumentException("Missing input:" + S3_LOCATION_FIELD);
    }

    private String removeRoot(String path) {
        return path.startsWith(PATH_DELIMITER) ? path.substring(1) : path;
    }
}
