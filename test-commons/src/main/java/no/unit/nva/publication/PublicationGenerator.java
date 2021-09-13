package no.unit.nva.publication;

import com.github.javafaker.Faker;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.model.AdditionalIdentifier;
import no.unit.nva.model.Approval;
import no.unit.nva.model.ApprovalStatus;
import no.unit.nva.model.ApprovalsBody;
import no.unit.nva.model.Contributor;
import no.unit.nva.model.EntityDescription;
import no.unit.nva.model.File;
import no.unit.nva.model.FileSet;
import no.unit.nva.model.Grant;
import no.unit.nva.model.Identity;
import no.unit.nva.model.License;
import no.unit.nva.model.NameType;
import no.unit.nva.model.Organization;
import no.unit.nva.model.Publication;
import no.unit.nva.model.PublicationDate;
import no.unit.nva.model.PublicationStatus;
import no.unit.nva.model.Reference;
import no.unit.nva.model.ResearchProject;
import no.unit.nva.model.ResearchProject.Builder;
import no.unit.nva.model.Role;
import no.unit.nva.model.contexttypes.Book;
import no.unit.nva.model.contexttypes.Journal;
import no.unit.nva.model.contexttypes.PublicationContext;
import no.unit.nva.model.contexttypes.Publisher;
import no.unit.nva.model.contexttypes.PublishingHouse;
import no.unit.nva.model.contexttypes.Series;
import no.unit.nva.model.contexttypes.UnconfirmedPublisher;
import no.unit.nva.model.exceptions.InvalidIsbnException;
import no.unit.nva.model.exceptions.MalformedContributorException;
import no.unit.nva.model.instancetypes.PublicationInstance;
import no.unit.nva.model.instancetypes.book.BookMonograph;
import no.unit.nva.model.instancetypes.book.BookMonographContentType;
import no.unit.nva.model.instancetypes.journal.JournalArticle;
import no.unit.nva.model.instancetypes.journal.JournalArticleContentType;
import no.unit.nva.model.pages.MonographPages;
import no.unit.nva.model.pages.Pages;
import no.unit.nva.model.pages.Range;
import nva.commons.core.attempt.Try;

import java.net.URI;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * Generates a Publication with no empty field, except for DoiRequest.
 */
@SuppressWarnings("PMD.CouplingBetweenObjects")
public final class PublicationGenerator {

    public static final String PUBLISHER_ID = "https://example.org/123";
    public static final String SAMPLE_ISBN = "1-56619-909-3";
    public static final String LEXVO_ENG = "https://lexvo.org/id/iso639-3/eng";
    public static final int SINGLE_CONTRIBUTOR = 1;
    public static final Instant ONE_MINUTE_IN_THE_PAST = Instant.now().minusSeconds(60L);
    private static final Faker FAKER = Faker.instance();
    private static final Random RANDOM = new Random(System.currentTimeMillis());
    private static final int MAX_INT = 100;
    public static final String SOME_URI = "https://www.example.org/";
    public static final String NVA_PUBLICATION_CHANNEL_URI = "https://testingnva.aws.unit.no/publication-channels/";
    public static final String SOME_PAGES = "33";

    private PublicationGenerator() {

    }

    public static Publication publicationWithIdentifier() {
        return generateJournalPublication(SortableIdentifier.next());
    }

    public static Publication createPublicationWithEntityDescription(EntityDescription entityDescription) {
        return generatePublicationWithEntityDescription(SortableIdentifier.next(), entityDescription);
    }

    /**
     * Generate a minimal Publication for testing.
     *
     * @param identifier Sortable identifier
     * @return publication
     */
    public static Publication generateJournalPublication(SortableIdentifier identifier) {

        EntityDescription entityDescription = createSampleJournalEntityDescription();

        return new Publication.Builder()
            .withIdentifier(identifier)
            .withCreatedDate(ONE_MINUTE_IN_THE_PAST)
            .withModifiedDate(ONE_MINUTE_IN_THE_PAST)
            .withOwner(randomEmail())
            .withStatus(PublicationStatus.DRAFT)
            .withPublisher(samplePublisher())
            .withEntityDescription(entityDescription)
            .withFileSet(sampleFileSet())
            .withDoi(randomUri())
            .withIndexedDate(randomDate().toInstant())
            .withLink(randomUri())
            .withProjects(randomProjects())
            .withHandle(randomUri())
            .withPublishedDate(randomDate().toInstant())
            .withAdditionalIdentifiers(randomAdditionalIdentifiers())
            .build();
    }

    /**
     * Generate a minimal Publication for testing.
     *
     * @param identifier Sortable identifier
     * @return publication
     */
    public static Publication generatePublicationWithEntityDescription(SortableIdentifier identifier,
                                                                       EntityDescription entityDescription) {

        Instant oneMinuteInThePast = Instant.now().minusSeconds(60L);

        List<ResearchProject> projects = randomProjects();
        return new Publication.Builder()
            .withIdentifier(identifier)
            .withCreatedDate(oneMinuteInThePast)
            .withModifiedDate(oneMinuteInThePast)
            .withOwner(randomEmail())
            .withStatus(PublicationStatus.DRAFT)
            .withPublisher(samplePublisher())
            .withEntityDescription(entityDescription)
            .withFileSet(sampleFileSet())
            .withDoi(randomUri())
            .withIndexedDate(randomDate().toInstant())
            .withLink(randomUri())
            .withProjects(projects)
            .withHandle(randomUri())
            .withPublishedDate(randomDate().toInstant())
            .withAdditionalIdentifiers(randomAdditionalIdentifiers())
            .build();
    }

    public static Contributor randomContributor(int sequence) throws MalformedContributorException {
        return new Contributor.Builder()
            .withIdentity(randomIdentity())
            .withAffiliations(List.of(randomOrganization()))
            .withEmail(randomEmail())
            .withSequence(sequence)
            .withRole(Role.CREATOR)
            .build();
    }

    public static String randomString() {
        return FAKER.lorem().sentence();
    }

    public static Organization randomOrganization() {
        return new Organization.Builder()
            .withId(randomUri())
            .withLabels(randomMap())
            .build();
    }

    public static URI randomUri() {
        return URI.create(SOME_URI + FAKER.lorem().word());
    }

    public static URI randomPublicationChannelsUri() {
        return URI.create(NVA_PUBLICATION_CHANNEL_URI + FAKER.lorem().word());
    }

    public static String randomEmail() {
        return FAKER.internet().emailAddress();
    }

    public static Identity randomIdentity() {
        return new Identity.Builder()
            .withName(randomEmail())
            .withId(randomUri())
            .withArpId(randomString())
            .withNameType(NameType.PERSONAL)
            .withOrcId(randomString())
            .build();
    }

    private static Set<AdditionalIdentifier> randomAdditionalIdentifiers() {
        return Set.of(new AdditionalIdentifier(randomString(), randomString()));
    }

    private static List<ResearchProject> randomProjects() {
        ResearchProject project = new Builder()
            .withId(randomUri())
            .withName(randomString())
            .withGrants(randomGrants())
            .withApprovals(randomApprovals())
            .build();

        return List.of(project);
    }

    private static List<Approval> randomApprovals() {
        Approval approval = new Approval.Builder()
            .withDate(randomDate().toInstant())
            .withApprovalStatus(ApprovalStatus.APPLIED)
            .withApprovedBy(ApprovalsBody.NARA)
            .withApplicationCode(randomString())
            .withDate(randomDate().toInstant())
            .build();
        return List.of(approval);
    }

    private static List<Grant> randomGrants() {
        Grant grant = new Grant.Builder()
            .withSource(randomString())
            .withId(randomString())
            .build();
        return List.of(grant);
    }

    private static Organization samplePublisher() {
        return new Organization.Builder()
            .withId(URI.create(PUBLISHER_ID))
            .withLabels(randomMap())
            .build();
    }

    private static FileSet sampleFileSet() {
        License licenseId = randomLicense();
        File file = randomFile(licenseId);
        return new FileSet.Builder().withFiles(List.of(file)).build();
    }

    private static License randomLicense() {
        return new License.Builder()
            .withIdentifier(randomString())
            .withLink(randomUri())
            .withLabels(randomMap())
            .build();
    }

    private static Map<String, String> randomMap() {
        return Map.of(randomString(), randomString());
    }

    private static File randomFile(License license) {
        return new File.Builder()
            .withIdentifier(UUID.randomUUID())
            .withLicense(license)
            .withName(randomString())
            .withSize(randomInteger().longValue())
            .withEmbargoDate(randomDate().toInstant())
            .withMimeType(randomString())
            .build();
    }

    private static EntityDescription createSampleJournalEntityDescription() {
        Contributor contributor = Try.attempt(() -> randomContributor(SINGLE_CONTRIBUTOR)).orElseThrow();

        return new EntityDescription.Builder()
            .withMainTitle(randomString())
            .withDate(randomPublicationDate())
            .withReference(randomReference(journalArticlePublicationInstance()))
            .withContributors(List.of(contributor))
            .withAbstract(randomString())
            .withAlternativeTitles(randomTitles())
            .withDescription(randomString())
            .withLanguage(URI.create(LEXVO_ENG))
            .withMetadataSource(randomUri())
            .withNpiSubjectHeading(randomString())
            .withTags(List.of(randomString(), randomString()))
            .build();
    }

    public static EntityDescription createSampleEntityDescriptionBook(URI bookSeriesId, PublishingHouse publisher)
            throws InvalidIsbnException {
        Contributor contributor = Try.attempt(() -> randomContributor(SINGLE_CONTRIBUTOR)).orElseThrow();

        final Book book = new Book(new Series(bookSeriesId), randomString(), publisher, List.of(randomISBN()));

        Map<String, String> alternativeTitles = randomTitles();
        List<String> tags = List.of(randomString(), randomString());
        Reference reference = bookReference(bookMonographPublicationInstance(), book);
        return new EntityDescription.Builder()
            .withMainTitle(randomString())
            .withDate(randomPublicationDate())
            .withReference(reference)
            .withContributors(List.of(contributor))
            .withAbstract(randomString())
            .withAlternativeTitles(alternativeTitles)
            .withDescription(randomString())
            .withLanguage(URI.create(LEXVO_ENG))
            .withMetadataSource(randomUri())
            .withNpiSubjectHeading(randomString())
            .withTags(tags)
            .build();
    }

    private static Map<String, String> randomTitles() {
        return Map.of(LEXVO_ENG, randomString());
    }

    private static PublicationDate randomPublicationDate() {
        return new PublicationDate.Builder()
            .withYear(randomYear())
            .withMonth(randomMonth())
            .withDay(randomDayOfMonth())
            .build();
    }

    private static Reference randomReference(PublicationInstance<? extends Pages> publicationInstance) {
        PublicationContext publicationContext = new Journal(randomPublicationChannelsUri().toString());
        return new Reference.Builder()
            .withPublicationInstance(publicationInstance)
            .withPublishingContext(publicationContext)
            .withDoi(randomUri())
            .build();
    }

    private static Reference bookReference(PublicationInstance<? extends Pages> publicationInstance, PublicationContext publicationContext) {
        return new Reference.Builder()
            .withPublicationInstance(publicationInstance)
            .withPublishingContext(publicationContext)
            .withDoi(randomUri())
            .build();
    }

    private static JournalArticle journalArticlePublicationInstance() {
        var startRange = randomInteger();
        var endRange = startRange + randomInteger() + 1;
        Range pages = new Range(startRange.toString(), Integer.toString(endRange));
        return new JournalArticle.Builder()
            .withArticleNumber(randomInteger().toString())
            .withIssue(randomMonth())
            .withVolume(randomString())
            .withPages(pages)
            .withContent(randomJournalArticleContentType())
            .build();
    }

    private static BookMonograph bookMonographPublicationInstance() {
        final Range range = new Range.Builder().withBegin(randomString()).build();
        final MonographPages pages = new MonographPages.Builder()
            .withPages(SOME_PAGES)
            .withIntroduction(range)
            .build();
        return new BookMonograph.Builder()
            .withContentType(BookMonographContentType.ACADEMIC_MONOGRAPH)
            .withPages(pages)
            .build();
    }

    private static JournalArticleContentType randomJournalArticleContentType() {
        return randomElement(JournalArticleContentType.values());
    }

    private static <T> T randomElement(T... values) {
        return values[RANDOM.nextInt(values.length)];
    }

    private static Integer randomInteger() {
        return RANDOM.nextInt(MAX_INT);
    }

    private static String randomDayOfMonth() {
        return Integer.toString(randomDate().get(Calendar.DAY_OF_MONTH));
    }

    private static String randomMonth() {
        return Integer.toString(randomDate().get(Calendar.MONTH));
    }

    private static String randomYear() {
        Calendar calendar = randomDate();
        return Integer.toString(calendar.get(Calendar.YEAR));
    }

    private static Calendar randomDate() {
        Date date = FAKER.date().birthday();
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        return calendar;
    }

    public static  String randomISBN() {
        return SAMPLE_ISBN;
    }

    public static PublishingHouse publishingHouseWithUri() {
        return new Publisher(randomPublicationChannelsUri());
    }

    public static PublishingHouse unconfirmedPublishingHouse() {
        return new UnconfirmedPublisher(randomString());
    }
}
