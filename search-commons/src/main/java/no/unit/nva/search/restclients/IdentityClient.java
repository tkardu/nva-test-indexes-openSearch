package no.unit.nva.search.restclients;

import no.unit.nva.search.restclients.responses.UserResponse;

import java.util.Optional;

public interface IdentityClient {

    Optional<UserResponse> getUser(String username);

}
