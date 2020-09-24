package no.unit.nva.dynamodb;

import no.unit.nva.model.Contributor;
import no.unit.nva.search.IndexContributor;

public class IndexContributorHelper extends IndexContributor {

    public IndexContributorHelper(Contributor contributor) {
        super(contributor.getIdentity().getArpId(),
            contributor.getIdentity().getName());
    }
}
