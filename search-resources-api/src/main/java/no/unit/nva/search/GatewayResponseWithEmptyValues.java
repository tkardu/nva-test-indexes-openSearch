package no.unit.nva.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import nva.commons.apigateway.exceptions.GatewayResponseSerializingException;
import nva.commons.core.JsonUtils;

import java.io.Serializable;
import java.util.Map;

public class GatewayResponseWithEmptyValues<T> implements Serializable {

    private final String body;
    private final Map<String, String> headers;
    private final int statusCode;

    /**
     * Constructor for JSON deserializing.
     *
     * @param body       the body as JSON string
     * @param headers    the headers map.
     * @param statusCode the status code.
     */
    @JsonCreator
    public GatewayResponseWithEmptyValues(
        @JsonProperty("body") final String body,
        @JsonProperty("headers") final Map<String, String> headers,
        @JsonProperty("statusCode") final int statusCode) {
        this.body = body;
        this.headers = headers;
        this.statusCode = statusCode;
    }

    /**
     * Constructor for GatewayResponse.
     *
     * @param body       body of response
     * @param headers    http headers for response
     * @param statusCode status code for response
     * @throws GatewayResponseSerializingException when serializing fails
     */
    public GatewayResponseWithEmptyValues(T body, Map<String, String> headers, int statusCode)
        throws GatewayResponseSerializingException {
        try {
            this.statusCode = statusCode;
            this.body = JsonUtils.objectMapperWithEmpty.writeValueAsString(body);
            this.headers = Map.copyOf(headers);
        } catch (JsonProcessingException e) {
            throw new GatewayResponseSerializingException(e);
        }
    }

    public String getBody() {
        return body;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
