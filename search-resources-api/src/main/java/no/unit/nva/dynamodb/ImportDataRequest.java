package no.unit.nva.dynamodb;

public class ImportDataRequest {
    private String s3bucket;
    private String s3key;

    public ImportDataRequest(String s3bucket, String s3key) {
        this.s3bucket = s3bucket;
        this.s3key = s3key;
    }

    public String getS3bucket() {
        return s3bucket;
    }

    public void setS3bucket(String s3bucket) {
        this.s3bucket = s3bucket;
    }

    public String getS3key() {
        return s3key;
    }

    public void setS3key(String s3key) {
        this.s3key = s3key;
    }
}
