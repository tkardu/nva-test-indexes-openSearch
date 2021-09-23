package no.unit.nva.search;

import java.util.List;

public class ProcessResult {

    private final List<String> failedResults;
    private final String listingStartingPoint;
    private final boolean truncated;

    public ProcessResult(List<String> failedResults, String listingStartingPoint, boolean truncated) {
        this.failedResults = failedResults;
        this.listingStartingPoint = listingStartingPoint;
        this.truncated = truncated;
    }

    public List<String> getFailedResults() {
        return failedResults;
    }

    public String getListingStartingPoint() {
        return listingStartingPoint;
    }

    public boolean isTruncated() {
        return truncated;
    }
}
