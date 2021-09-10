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
import no.unit.nva.model.contexttypes.BookSeries;
import no.unit.nva.model.contexttypes.Journal;
import no.unit.nva.model.contexttypes.PublicationContext;
import no.unit.nva.model.contexttypes.Publisher;
import no.unit.nva.model.contexttypes.PublishingHouse;
import no.unit.nva.model.contexttypes.Series;
import no.unit.nva.model.contexttypes.UnconfirmedPublisher;
import no.unit.nva.model.exceptions.InvalidIsbnException;
import no.unit.nva.model.exceptions.InvalidIssnException;
import no.unit.nva.model.exceptions.InvalidUnconfirmedSeriesException;
import no.unit.nva.model.exceptions.MalformedContributorException;
import no.unit.nva.model.instancetypes.PublicationInstance;
import no.unit.nva.model.instancetypes.book.BookMonograph;
import no.unit.nva.model.instancetypes.book.BookMonographContentType;
import no.unit.nva.model.instancetypes.journal.JournalArticle;
import no.unit.nva.model.instancetypes.journal.JournalArticleContentType;
import no.unit.nva.model.pages.MonographPages;
import no.unit.nva.model.pages.Pages;
import no.unit.nva.model.pages.Range;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.attempt.Try;

import java.net.MalformedURLException;
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

    public static final String PUBLISHER_ID = "http://example.org/123";
    public static final boolean OPEN_ACCESS = true;
    public static final boolean PEER_REVIEWED = true;
    public static final String SAMPLE_ISSN = "2049-3630";
    public static final String SAMPLE_ISBN = "1-56619-909-3";
    public static final String LEXVO_ENG = "http://lexvo.org/id/iso639-3/eng";
    public static final int SINGLE_CONTRIBUTOR = 1;
    private static final Faker FAKER = Faker.instance();
    private static final Random RANDOM = new Random(System.currentTimeMillis());
    private static final int MAX_INT = 100;

    @JacocoGenerated
    private PublicationGenerator() {

    }

    @JacocoGenerated
    public static Publication publicationWithIdentifier() throws MalformedURLException, InvalidIssnException {
        return generateJournalPublication(SortableIdentifier.next());
    }
    @JacocoGenerated
    public static Publication createPublicationWithEntityDescription(EntityDescription entityDescription) {
        return generatePublicationWithEntityDescription(SortableIdentifier.next(), entityDescription);
    }

    /**
     * Generate a minimal Publication for testing.
     *
     * @param identifier Sortable identifier
     * @return publication
     */
    @JacocoGenerated
    public static Publication generateJournalPublication(SortableIdentifier identifier)
        throws MalformedURLException, InvalidIssnException {

        EntityDescription entityDescription = createSampleJournalEntityDescription();

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

    /**
     * Generate a minimal Publication for testing.
     *
     * @param identifier Sortable identifier
     * @return publication
     */
    @JacocoGenerated
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
        return URI.create("https://www.example.org/" + FAKER.lorem().word());
    }

    public static URI randomPublicationChannelsUri() {
        return URI.create("https://testingnva.aws.unit.no/publication-channels/" + FAKER.lorem().word());
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
        List<Grant> grants = randomGrants();
        List<Approval> approvals = randomApprovals();
        ResearchProject project = new Builder()
            .withId(randomUri())
            .withName(randomString())
            .withGrants(grants)
            .withApprovals(approvals)
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

    private static EntityDescription createSampleJournalEntityDescription()
        throws MalformedURLException, InvalidIssnException {
        Contributor contributor = Try.attempt(() -> randomContributor(SINGLE_CONTRIBUTOR)).orElseThrow();

        PublicationInstance<? extends Pages> publicationInstance = journalArticlePublicationInstance();
        Reference reference = randomReference(publicationInstance);
        PublicationDate publicationDate = randomPublicationDate();

        Map<String, String> alternativeTitles = randomTitles();
        List<String> tags = List.of(randomString(), randomString());
        return new EntityDescription.Builder()
            .withMainTitle(randomString())
            .withDate(publicationDate)
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

    public static EntityDescription createSampleEntityDescriptionBook(URI bookSeriesId, PublishingHouse publisher)
            throws InvalidIsbnException, InvalidUnconfirmedSeriesException {
        Contributor contributor = Try.attempt(() -> randomContributor(SINGLE_CONTRIBUTOR)).orElseThrow();
        BookSeries bookSeries = new Series(bookSeriesId);

        PublicationInstance<? extends Pages> publicationInstance = bookMonographPublicationInstance();

        final Book book = new Book(bookSeries, randomString(), publisher, List.of(randomISBN()));

        Reference reference = bookReference(publicationInstance, book);
        PublicationDate publicationDate = randomPublicationDate();

        Map<String, String> alternativeTitles = randomTitles();
        List<String> tags = List.of(randomString(), randomString());
        return new EntityDescription.Builder()
                .withMainTitle(randomString())
                .withDate(publicationDate)
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
        return new PublicationDate.Builder().withYear(randomYear()).withMonth(randomMonth()).withDay(
            randomDayOfMonth()).build();
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
        final MonographPages pages = new MonographPages.Builder().withPages("33").withIntroduction(range).build();
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

