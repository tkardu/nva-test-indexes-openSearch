import no.unit.nva.search.RequestUtil;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.handlers.RequestInfo;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RequestUtilTest {

    public static final String SAMPLE_QUERY_PARAMETER = "SampleQueryParameter";

    @Test
    public void canSearchTermRequest() throws ApiGatewayException {
        RequestInfo requestInfo = new RequestInfo();
        requestInfo.setQueryParameters(Map.of(RequestUtil.SEARCH_TERM_KEY, SAMPLE_QUERY_PARAMETER));
        String queryParam = RequestUtil.getSearchTerm(requestInfo);
        assertEquals(SAMPLE_QUERY_PARAMETER,  queryParam);
    }
}
