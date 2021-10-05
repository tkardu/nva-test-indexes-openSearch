package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.search.constants.ApplicationConstants;
import no.unit.nva.testutils.IoUtils;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkResponse;

import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

public class BatchIndexTest {

    public static final String FIRST_FILE_HAS_NO_PUBLISHED_PUBLICATIONS = "input_without_published_publications.ion.gz";
    public static final String SECOND_FILE_HAS_PUBLISHED_PUBLICATIONS = "input_with_published_publications.ion.gz";

    public static final String[] RESOURCES = {
        FIRST_FILE_HAS_NO_PUBLISHED_PUBLICATIONS,
        SECOND_FILE_HAS_PUBLISHED_PUBLICATIONS
    };

    public static final String FILE_WITH_IDENTIFIERS_OF_PUBLISHED_RESOURCES_IN_SAMPLE_FILES =
        "published_resources_identifiers_for_all_files.txt";
    public static final Context CONTEXT = mock(Context.class);
    public static final String[] PUBLISHED_RESOURCES_IDENTIFIERS = readPublishedResourcesIdentifiersFromStaticFile();
    public static final Random RANDOM = new Random();
    public static final int ARBITRARY_QUERY_TIME = 123;

    protected StubElasticSearchHighLevelRestClient failingElasticSearchClient() {
        return new StubElasticSearchHighLevelRestClient() {
            @Override
            public Stream<BulkResponse> batchInsert(Stream<IndexDocument> indexDocuments) {
                List<BulkItemResponse> itemResponses = indexDocuments
                    .map(IndexDocument::getIdentifier)
                    .map(id -> createFailure(id))
                    .map(fail -> new BulkItemResponse(randomNumber(), OpType.UPDATE, fail))
                    .collect(Collectors.toList());
                BulkResponse response =
                    new BulkResponse(itemResponses.toArray(BulkItemResponse[]::new), ARBITRARY_QUERY_TIME);
                return Stream.of(response);
            }
        };
    }

    private static String[] readPublishedResourcesIdentifiersFromStaticFile() {
        return IoUtils.linesfromResource(Path.of(FILE_WITH_IDENTIFIERS_OF_PUBLISHED_RESOURCES_IN_SAMPLE_FILES))
            .toArray(String[]::new);
    }

    private Failure createFailure(SortableIdentifier id) {
        return new Failure(ApplicationConstants.ELASTICSEARCH_ENDPOINT_INDEX, "failureType",
                           id.toString(), new Exception("failingBulkIndexMessage"));
    }

    private int randomNumber() {
        return RANDOM.nextInt();
    }
}
