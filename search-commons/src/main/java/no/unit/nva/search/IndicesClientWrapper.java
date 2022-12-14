package no.unit.nva.search;

import nva.commons.core.JacocoGenerated;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;

import java.io.IOException;

/**
 * Wrapper class for being able to test calls to the final class IndicesClient.
 */
@JacocoGenerated
public class IndicesClientWrapper {

    private final IndicesClient indicesClient;

    public IndicesClientWrapper(IndicesClient indices) {
        this.indicesClient = indices;
    }

    /**
     * Do not use this method. This method is only for experimenting. If you want to use a method of {@link
     * IndicesClient} replicate the method in {@link IndicesClientWrapper} and call the respective
     * {@link IndicesClient} one.
     *
     * @return the contained client.
     */
    public IndicesClient getIndicesClient() {
        return indicesClient;
    }

    public CreateIndexResponse create(CreateIndexRequest createIndexRequest, RequestOptions requestOptions)
            throws IOException {
        return indicesClient.create(createIndexRequest, requestOptions);
    }

    public AcknowledgedResponse putSettings(UpdateSettingsRequest updateSettingsRequest, RequestOptions requestOptions)
            throws IOException {
        return indicesClient.putSettings(updateSettingsRequest, requestOptions);
    }

    public GetIndexResponse get(GetIndexRequest getIndexRequest, RequestOptions requestOptions) throws IOException {
        return indicesClient.get(getIndexRequest, requestOptions);
    }
}
