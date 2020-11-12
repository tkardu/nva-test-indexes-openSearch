package no.unit.nva.dynamodb;

public class ImportDataRequest {
    private String s3bucket;
    private String s3folderkey;

    public ImportDataRequest() {
    }

    public ImportDataRequest(String s3bucket, String s3folderkey) {
        this.s3bucket = s3bucket;
        this.s3folderkey = s3folderkey;
    }

    public String getS3bucket() {
        return s3bucket;
    }

    public void setS3bucket(String s3bucket) {
        this.s3bucket = s3bucket;
    }

    public String getS3folderkey() {
        return s3folderkey;
    }

    public void setS3folderkey(String s3folderkey) {
        this.s3folderkey = s3folderkey;
    }
}
