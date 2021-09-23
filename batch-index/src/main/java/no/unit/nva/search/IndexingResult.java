package no.unit.nva.search;

import java.util.List;

public class IndexingResult {

    private final List<String> failedResults;
    private final String nestStartMarker;
    private final boolean truncated;

    public IndexingResult(List<String> failedResults, String nextStartMarker, boolean truncated) {
        this.failedResults = failedResults;
        this.nestStartMarker = nextStartMarker;
        this.truncated = truncated;
    }

    public List<String> getFailedResults() {
        return failedResults;
    }

    public String getNextStartMarker() {
        return nestStartMarker;
    }

    public boolean isTruncated() {
        return truncated;
    }
}
