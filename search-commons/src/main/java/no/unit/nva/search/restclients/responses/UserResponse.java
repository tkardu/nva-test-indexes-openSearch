package no.unit.nva.search.restclients.responses;

import static no.unit.nva.search.constants.ApplicationConstants.objectMapperNoEmpty;
import static nva.commons.core.attempt.Try.attempt;
import com.fasterxml.jackson.core.JsonProcessingException;

public class UserResponse {

    private ViewingScope viewingScope;

    public ViewingScope getViewingScope() {
        return viewingScope != null ? viewingScope : new ViewingScope();
    }

    public void setViewingScope(ViewingScope viewingScope) {
        this.viewingScope = viewingScope;
    }

    public static UserResponse fromJson(String json) throws JsonProcessingException {
        return objectMapperNoEmpty.readValue(json, UserResponse.class);
    }

    public String toJson() {
        return attempt(() -> objectMapperNoEmpty.writeValueAsString(this)).orElseThrow();
    }
}
