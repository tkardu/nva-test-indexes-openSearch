package no.unit.nva.dynamodb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import nva.commons.utils.JacocoGenerated;

public class ImportDataRequest {

    private final String s3bucket;
    private final String s3folderkey;

    @JacocoGenerated
    @JsonCreator
    public ImportDataRequest(@JsonProperty("s3bucket") String s3bucket,
                             @JsonProperty("s3folderkey") String s3folderkey) {
        this.s3bucket = s3bucket;
        this.s3folderkey = s3folderkey;
    }

    protected ImportDataRequest(Builder builder) {
        this.s3bucket = builder.s3bucket;
        this.s3folderkey = builder.s3folderkey;
    }

    public String getS3bucket() {
        return s3bucket;
    }

    public String getS3folderkey() {
        return s3folderkey;
    }

    public static final class Builder {

        private String s3bucket;
        private String s3folderkey;

        public Builder() {
        }

        public ImportDataRequest.Builder withS3Bucket(String s3bucket) {
            this.s3bucket = s3bucket;
            return this;
        }


        public ImportDataRequest.Builder withS3FolderKey(String s3folderkey) {
            this.s3folderkey = s3folderkey;
            return this;
        }

        public ImportDataRequest build() {
            return new ImportDataRequest(this);
        }

    }


}
