package org.rra.cstore;

import org.dcm4che3.data.UID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.rra.cstore.DicomStoreBatch.Destination;
import org.rra.cstore.DicomStoreBatch.Instance;
import org.rra.cstore.DicomStoreBatch.Outcome;
import org.rra.cstore.DicomStoreBatch.Result;
import org.rra.dcmconfig.DcmConfig;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * C-STORE against a real dcm4che SCP on localhost. Every test checks that an
 * instance counts as stored only if the SCP really received it.
 */
public class DicomStoreBatchTest {

    private static final String CALLING = "IDSC_ANONYMIZER";

    private final List<TestStoreScp> scps = new ArrayList<>();

    @AfterEach
    public void stopScps() {
        scps.forEach(TestStoreScp::close);
        scps.clear();
    }

    private TestStoreScp scp(TestStoreScp scp) {
        scps.add(scp);
        return scp;
    }

    private static DcmConfig config() {
        DcmConfig cfg = new DcmConfig();
        cfg.CONNECT_TIMEOUT = 5000;
        cfg.RESPONSE_TIMEOUT = 10000;
        return cfg;
    }

    private static Instance of(TestDicom.Instance dicom) {
        return () -> new ByteArrayInputStream(dicom.bytes());
    }

    private static Instance of(byte[] bytes) {
        return () -> new ByteArrayInputStream(bytes);
    }

    private static List<TestDicom.Instance> cts(int count) throws IOException {
        List<TestDicom.Instance> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(TestDicom.ct(1024));
        }
        return list;
    }

    private static List<Instance> instances(List<TestDicom.Instance> dicoms) {
        return dicoms.stream().map(DicomStoreBatchTest::of).toList();
    }

    private static Destination to(TestStoreScp scp, String calledAet) {
        return new Destination("127.0.0.1", scp.port(), CALLING, calledAet);
    }

    @Test
    public void storesBatchOverOneAssociation() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("PROJ_A"));
        List<TestDicom.Instance> dicoms = cts(50);

        List<Result> results = new DicomStoreBatch(config()).send(to(scp, "PROJ_A"), instances(dicoms));

        assertEquals(50, results.size());
        for (int i = 0; i < 50; i++) {
            assertEquals(Outcome.SUCCESS, results.get(i).outcome(), "instance " + i);
            assertEquals(0, results.get(i).status());
            assertEquals(dicoms.get(i).iuid(), results.get(i).sopInstanceUid());
        }
        assertEquals(50, scp.received().size());
        assertEquals(1, scp.associationCount(), "one association for the whole batch");
        assertEquals("PROJ_A", scp.received().get(0).calledAet());
        assertEquals(CALLING, scp.received().get(0).callingAet());
    }

    @Test
    public void failureStatusFailsOnlyThatInstance() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("PROJ_A"));
        List<TestDicom.Instance> dicoms = cts(5);
        scp.failWith(dicoms.get(2).iuid(), 0xA700);

        List<Result> results = new DicomStoreBatch(config()).send(to(scp, "PROJ_A"), instances(dicoms));

        assertEquals(Outcome.FAILED, results.get(2).outcome());
        assertEquals(DicomStoreBatch.STORE_REJECTED, results.get(2).errorCode());
        assertEquals(0xA700, results.get(2).status());
        assertTrue(results.get(2).message().contains("A700"), results.get(2).message());
        for (int i : new int[] {0, 1, 3, 4}) {
            assertEquals(Outcome.SUCCESS, results.get(i).outcome(), "instance " + i);
        }
        assertEquals(4, scp.received().size());
    }

    @Test
    public void warningStatusCountsAsStored() {
        assertEquals(Outcome.WARNING, DicomStoreBatch.classify(0xB000));
        assertEquals(Outcome.WARNING, DicomStoreBatch.classify(0xB007));
        assertEquals(Outcome.WARNING, DicomStoreBatch.classify(0x0107));
        assertEquals(Outcome.SUCCESS, DicomStoreBatch.classify(0x0000));
        assertEquals(Outcome.FAILED, DicomStoreBatch.classify(0xA700));
        assertEquals(Outcome.FAILED, DicomStoreBatch.classify(0xA900));
        assertEquals(Outcome.FAILED, DicomStoreBatch.classify(0xC000));
        assertEquals(Outcome.FAILED, DicomStoreBatch.classify(0x0110));
        assertEquals(Outcome.FAILED, DicomStoreBatch.classify(0x0122));
        assertEquals(Outcome.FAILED, DicomStoreBatch.classify(-1));
    }

    @Test
    public void rejectedSopClassIsNotReportedAsStored() throws Exception {
        // SCP accepts only MR, the batch is CT
        TestStoreScp scp = scp(new TestStoreScp("PROJ_A", new String[] {UID.MRImageStorage}, "*"));
        List<TestDicom.Instance> dicoms = cts(3);

        List<Result> results = new DicomStoreBatch(config()).send(to(scp, "PROJ_A"), instances(dicoms));

        for (Result result : results) {
            assertFalse(result.stored());
            // dcm4che rejects the association when no presentation context is acceptable
            assertTrue(DicomStoreBatch.NO_PRESENTATION_CONTEXT.equals(result.errorCode())
                            || DicomStoreBatch.ASSOCIATION_REJECTED.equals(result.errorCode()),
                    result.errorCode() + ": " + result.message());
        }
        assertEquals(0, scp.received().size());
    }

    @Test
    public void rejectedSopClassDoesNotStopTheOthers() throws Exception {
        TestStoreScp scp = scp(new TestStoreScp("PROJ_A", new String[] {UID.CTImageStorage}, "*"));
        TestDicom.Instance ct = TestDicom.ct(512);
        TestDicom.Instance mr = TestDicom.part10(UID.MRImageStorage, UID.ExplicitVRLittleEndian, 512);

        List<Result> results = new DicomStoreBatch(config()).send(to(scp, "PROJ_A"), List.of(of(ct), of(mr)));

        assertEquals(Outcome.SUCCESS, results.get(0).outcome());
        assertEquals(DicomStoreBatch.NO_PRESENTATION_CONTEXT, results.get(1).errorCode());
        assertEquals(1, scp.received().size());
        assertEquals(ct.iuid(), scp.received().get(0).iuid());
    }

    @Test
    public void transferSyntaxNotAcceptedIsNotSkippedSilently() throws Exception {
        // CT accepted, but only in explicit VR little endian
        TestStoreScp scp = scp(new TestStoreScp("PROJ_A", new String[] {UID.CTImageStorage},
                UID.ExplicitVRLittleEndian));
        TestDicom.Instance plain = TestDicom.ct(512);
        TestDicom.Instance jpeg = TestDicom.part10(UID.CTImageStorage, UID.JPEGLosslessSV1, 512);

        List<Result> results = new DicomStoreBatch(config()).send(to(scp, "PROJ_A"), List.of(of(plain), of(jpeg)));

        assertEquals(Outcome.SUCCESS, results.get(0).outcome());
        assertEquals(DicomStoreBatch.TRANSFER_SYNTAX_NOT_ACCEPTED, results.get(1).errorCode());
        assertEquals(1, scp.received().size());
    }

    @Test
    public void wrongCalledAeIsRejected() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("PROJ_A"));

        List<Result> results = new DicomStoreBatch(config()).send(to(scp, "PROJ_B"), instances(cts(2)));

        for (Result result : results) {
            assertEquals(DicomStoreBatch.ASSOCIATION_REJECTED, result.errorCode(), result.message());
        }
        assertEquals(0, scp.received().size());
        // Log line of StoreScu names the reason, not just the count
        String summary = DicomStoreBatch.failureSummary(results);
        assertTrue(summary.startsWith("ASSOCIATION_REJECTED x2 ("), summary);
    }

    @Test
    public void unreachableScpFailsAllWithConnectionError() throws Exception {
        TestStoreScp scp = TestStoreScp.acceptingAll("PROJ_A");
        int port = scp.port();
        scp.close(); // nothing listens any more

        List<Result> results = new DicomStoreBatch(config()).send(
                new Destination("127.0.0.1", port, CALLING, "PROJ_A"), instances(cts(3)));

        for (Result result : results) {
            assertEquals(DicomStoreBatch.CONNECTION_FAILED, result.errorCode(), result.message());
        }
    }

    @Test
    public void abortMidBatchKeepsConfirmedAndFailsTheRest() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("PROJ_A"));
        List<TestDicom.Instance> dicoms = cts(6);
        scp.abortOn(dicoms.get(3).iuid());

        List<Result> results = new DicomStoreBatch(config()).send(to(scp, "PROJ_A"), instances(dicoms));

        for (int i = 0; i < 3; i++) {
            assertEquals(Outcome.SUCCESS, results.get(i).outcome(), "instance " + i);
        }
        for (int i = 3; i < 6; i++) {
            assertEquals(DicomStoreBatch.ASSOCIATION_ABORTED, results.get(i).errorCode(),
                    "instance " + i + ": " + results.get(i).message());
        }
        assertEquals(3, scp.received().size());
    }

    @Test
    public void invalidInputFailsOnlyThatInstance() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("PROJ_A"));
        TestDicom.Instance good = TestDicom.ct(256);

        List<Result> results = new DicomStoreBatch(config()).send(to(scp, "PROJ_A"), List.of(
                of(new byte[0]),
                of("not dicom at all".getBytes()),
                of(good)));

        assertEquals(DicomStoreBatch.INVALID_DICOM, results.get(0).errorCode());
        assertEquals(DicomStoreBatch.INVALID_DICOM, results.get(1).errorCode());
        assertNull(results.get(1).sopInstanceUid());
        assertEquals(Outcome.SUCCESS, results.get(2).outcome());
        assertEquals(1, scp.received().size());
    }

    @Test
    public void readErrorWhileSendingIsAFailure() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("PROJ_A"));
        TestDicom.Instance dicom = TestDicom.ct(4096);
        AtomicInteger opens = new AtomicInteger();
        // Header readable, stream breaks while the data set is sent
        Instance breaking = () -> {
            if (opens.incrementAndGet() == 1) {
                return new ByteArrayInputStream(dicom.bytes());
            }
            return new InputStream() {
                private final InputStream delegate = new ByteArrayInputStream(dicom.bytes());
                private int read;

                @Override
                public int read() throws IOException {
                    if (++read > 600) {
                        throw new IOException("content repository read failed");
                    }
                    return delegate.read();
                }
            };
        };

        List<Result> results = new DicomStoreBatch(config()).send(to(scp, "PROJ_A"), List.of(breaking));

        assertFalse(results.get(0).stored(), results.get(0).toString());
        assertEquals(0, scp.received().size());
    }

    @Test
    public void largeInstanceIsStreamedCompletely() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("PROJ_A"));
        TestDicom.Instance big = TestDicom.ct(20 * 1024 * 1024);

        List<Result> results = new DicomStoreBatch(config()).send(to(scp, "PROJ_A"), List.of(of(big)));

        assertEquals(Outcome.SUCCESS, results.get(0).outcome());
        assertEquals(1, scp.received().size());
        assertTrue(scp.received().get(0).bytes() > 20L * 1024 * 1024);
    }

    @Test
    public void manySopClassesAreSplitIntoSeveralAssociations() {
        List<DicomStoreBatch.Header> headers = new ArrayList<>();
        for (int i = 0; i < 130; i++) {
            headers.add(new DicomStoreBatch.Header(i, "1.2.3." + i, "2.25." + i, UID.ExplicitVRLittleEndian));
        }
        headers.add(new DicomStoreBatch.Header(130, "1.2.3.0", "2.25.130", UID.ExplicitVRLittleEndian));

        List<List<DicomStoreBatch.Header>> chunks = DicomStoreBatch.chunks(headers);

        assertEquals(2, chunks.size());
        assertEquals(128, chunks.get(0).size());
        assertEquals(3, chunks.get(1).size());
    }

    @Test
    public void failureSummaryCountsPerErrorCodeWithFirstMessage() {
        List<Result> results = List.of(
                Result.stored(0x0000, null, "2.25.1"),
                Result.failed(DicomStoreBatch.STORE_REJECTED, 0xA700, "C-STORE status A700: Out of resources", "2.25.2"),
                Result.failed(DicomStoreBatch.STORE_REJECTED, 0xA700, "C-STORE status A700: second", "2.25.3"),
                Result.failed(DicomStoreBatch.NO_PRESENTATION_CONTEXT, "SOP Class 1.2.3 not accepted by X", "2.25.4"),
                Result.failed(null, null, null));

        assertEquals("STORE_REJECTED x2 (C-STORE status A700: Out of resources); "
                        + "NO_PRESENTATION_CONTEXT x1 (SOP Class 1.2.3 not accepted by X); UNKNOWN x1",
                DicomStoreBatch.failureSummary(results));
    }

    @Test
    public void failureSummaryIsNullWhenAllStored() {
        assertNull(DicomStoreBatch.failureSummary(List.of(
                Result.stored(0x0000, null, "2.25.1"), Result.stored(0xB000, "warning", "2.25.2"))));
        assertNull(DicomStoreBatch.failureSummary(List.of()));
    }

    @Test
    public void failureSummaryShortensLongMessages() {
        String summary = DicomStoreBatch.failureSummary(List.of(
                Result.failed(DicomStoreBatch.ASSOCIATION_ABORTED, "x".repeat(1000), "2.25.1")));
        assertEquals("ASSOCIATION_ABORTED x1 (" + "x".repeat(300) + "...)", summary);
    }

    @Test
    public void emptyBatchSendsNothing() {
        assertTrue(new DicomStoreBatch(config())
                .send(new Destination("127.0.0.1", 104, CALLING, "PROJ_A"), List.of()).isEmpty());
    }
}
