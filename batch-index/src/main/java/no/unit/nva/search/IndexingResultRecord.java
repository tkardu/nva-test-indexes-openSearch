package no.unit.nva.search;

import java.util.List;

public class IndexingResultRecord<T> implements IndexingResult<T>{

    private final String nextStartMarker;
    private final List<T> failedResults;
    private final boolean truncated;

    public IndexingResultRecord(List<T> failedResults,String nextStartMarker, boolean truncated) {
        this.nextStartMarker = nextStartMarker;
        this.failedResults = failedResults;
        this.truncated = truncated;
    }

    @Override
    public List<T> getFailedResults() {
        return this.failedResults;
    }

    @Override
    public String getNextStartMarker() {
        return this.nextStartMarker;
    }

    @Override
    public boolean isTruncated() {
        return this.truncated;
    }
}
