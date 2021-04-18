package no.unit.nva.search;

import static nva.commons.core.attempt.Try.attempt;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import no.unit.nva.dataimport.S3IonReader;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.model.Publication;
import no.unit.nva.model.PublicationStatus;
import no.unit.nva.publication.storage.model.Resource;
import no.unit.nva.publication.storage.model.daos.DynamoEntry;
import no.unit.nva.publication.storage.model.daos.ResourceDao;
import no.unit.nva.s3.S3Driver;
import no.unit.nva.search.exception.SearchException;
import nva.commons.core.JsonUtils;
import nva.commons.core.attempt.Try;
import nva.commons.core.ioutils.IoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportToSearchIndexHandler implements RequestStreamHandler {

    private static final Logger logger = LoggerFactory.getLogger(ImportToSearchIndexHandler.class);
    private final S3IonReader ionReader;
    private final ElasticSearchHighLevelRestClient elasticSearchRestClient;
    private final S3Driver s3Driver;

    public ImportToSearchIndexHandler(S3Driver s3Driver, S3IonReader ionReader,
                                      ElasticSearchHighLevelRestClient elasticSearchRestClient) {
        this.s3Driver = s3Driver;
        this.ionReader = ionReader;
        this.elasticSearchRestClient = elasticSearchRestClient;
    }

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        String inputString = IoUtils.streamToString(input);
        ImportDataRequest request = JsonUtils.objectMapper.readValue(inputString, ImportDataRequest.class);
        Stream<Publication> publishedPublications = fetchPublishedPublicationsFromDynamoDbExportInS3(request);

        List<Try<SortableIdentifier>> indexActions = insertToIndex(publishedPublications)
                                                         .collect(Collectors.toList());

        List<String> failures = collectFailures(indexActions.stream());
        failures.forEach(this::logFailure);
        writeOutput(output, failures);
    }

    protected void writeOutput(OutputStream outputStream, List<String> failures)
        throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
            String outputJson = JsonUtils.objectMapperWithEmpty.writeValueAsString(failures);
            writer.write(outputJson);
        }
    }

    private void logFailure(String failureMessage) {
        logger.warn("Failed to index resource:" + failureMessage);
    }

    private Stream<Publication> fetchPublishedPublicationsFromDynamoDbExportInS3(ImportDataRequest request) {
        List<String> allFiles = s3Driver.listFiles(Path.of(request.getS3Path()));
        List<JsonNode> allContent = fetchAllContentFromDataExport(allFiles);
        return keepOnlyPublishedPublications(allContent);
    }

    private List<String> collectFailures(Stream<Try<SortableIdentifier>> indexActions) {
        return indexActions
                   .filter(Try::isFailure)
                   .map(f -> exceptionToString(f.getException()))
                   .collect(Collectors.toList());
    }

    private Stream<Try<SortableIdentifier>> insertToIndex(Stream<Publication> publishedPublications) {
        return publishedPublications
                   .map(IndexDocument::fromPublication)
                   .map(attempt(this::indexDocument));
    }

    private String exceptionToString(Exception exception) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(stringWriter);
        exception.printStackTrace(writer);
        return stringWriter.toString();
    }

    private SortableIdentifier indexDocument(IndexDocument doc) throws SearchException {
        elasticSearchRestClient.addDocumentToIndex(doc);
        return doc.getId();
    }

    private Stream<Publication> keepOnlyPublishedPublications(List<JsonNode> allContent) {
        Stream<DynamoEntry> dynamoEntries = allContent.stream().map(this::toDynamoEntry);
        Stream<Publication> allPublications = dynamoEntries
                                                  .filter(entry -> entry instanceof ResourceDao)
                                                  .map(dao -> (ResourceDao) dao)
                                                  .map(ResourceDao::getData)
                                                  .map(Resource::toPublication);
        return allPublications
                   .filter(publication -> PublicationStatus.PUBLISHED.equals(publication.getStatus()));
    }

    private DynamoEntry toDynamoEntry(JsonNode jsonNode) {
        return JsonUtils.objectMapperNoEmpty.convertValue(jsonNode, DynamoEntry.class);
    }

    private List<JsonNode> fetchAllContentFromDataExport(List<String> allFiles) {
        return allFiles.stream()
                   .map(attempt(ionReader::extractJsonNodeStreamFromS3File))
                   .map(Try::toOptional)
                   .flatMap(Optional::stream)
                   .flatMap(Function.identity())
                   .collect(Collectors.toList());
    }
}
