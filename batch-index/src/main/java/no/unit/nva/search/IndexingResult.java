package no.unit.nva.search;

import java.util.List;

public interface IndexingResult<T> {

    List<T> getFailedResults();

    String getNextStartMarker();

    boolean isTruncated();


}
