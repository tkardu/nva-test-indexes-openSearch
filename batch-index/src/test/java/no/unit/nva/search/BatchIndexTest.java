package no.unit.nva.search;

import static org.mockito.Mockito.mock;
import com.amazonaws.services.lambda.runtime.Context;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import no.unit.nva.indexing.testutils.FakeIndexingClient;
import no.unit.nva.search.constants.ApplicationConstants;
import no.unit.nva.search.models.IndexDocument;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkResponse;

public class BatchIndexTest {


    public static final Context CONTEXT = mock(Context.class);
    public static final Random RANDOM = new Random();
    public static final int ARBITRARY_QUERY_TIME = 123;

    protected FakeIndexingClient failingElasticSearchClient() {
        return new FakeIndexingClient() {
            @Override
            public Stream<BulkResponse> batchInsert(Stream<IndexDocument> indexDocuments) {
                List<BulkItemResponse> itemResponses = indexDocuments
                    .map(IndexDocument::getDocumentIdentifier)
                    .map(id -> createFailure(id))
                    .map(fail -> new BulkItemResponse(randomNumber(), OpType.UPDATE, fail))
                    .collect(Collectors.toList());
                BulkResponse response =
                    new BulkResponse(itemResponses.toArray(BulkItemResponse[]::new), ARBITRARY_QUERY_TIME);
                return Stream.of(response);
            }
        };
    }

    private Failure createFailure(String identifier) {
        return new Failure(ApplicationConstants.ELASTICSEARCH_ENDPOINT_INDEX, "failureType",
                           identifier, new Exception("failingBulkIndexMessage"));
    }

    private int randomNumber() {
        return RANDOM.nextInt();
    }
}
