package org.rra.processors;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcm4che3.data.UID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.rra.cstore.DicomStoreBatch;
import org.rra.cstore.TestDicom;
import org.rra.cstore.TestStoreScp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * StoreScu processor against a real dcm4che SCP on localhost: batching per
 * destination, routing per FlowFile, error attributes.
 */
public class StoreScuBatchTest {

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

    private static TestRunner runner(int port, String calledAet) {
        TestRunner runner = TestRunners.newTestRunner(StoreScu.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(StoreScu.REMOTE_HOST, "127.0.0.1");
        runner.setProperty(StoreScu.PORT, String.valueOf(port));
        runner.setProperty(StoreScu.CALLED_AET, calledAet);
        runner.setProperty(StoreScu.CALLING_AET, "IDSC_ANONYMIZER");
        runner.setProperty(StoreScu.CONNECT_TIMEOUT, "5000");
        runner.setProperty(StoreScu.RESPONSE_TIMEOUT, "10000");
        return runner;
    }

    @Test
    public void sendsAllFlowFilesOfARunOverOneAssociation() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("PROJ_A"));
        TestRunner runner = runner(scp.port(), "PROJ_A");
        for (int i = 0; i < 10; i++) {
            runner.enqueue(TestDicom.ct(512).bytes(), Map.of("dcma.order.id", "order-" + i));
        }

        runner.run();

        runner.assertAllFlowFilesTransferred(StoreScu.REL_SUCCESS, 10);
        for (MockFlowFile flowFile : runner.getFlowFilesForRelationship(StoreScu.REL_SUCCESS)) {
            flowFile.assertAttributeEquals(StoreScu.STORE_STATUS, "0000");
            flowFile.assertAttributeNotExists(StoreScu.STORE_ERROR_CODE);
            flowFile.assertAttributeExists("dcma.order.id");
        }
        assertEquals(10, scp.received().size());
        assertEquals(1, scp.associationCount());
    }

    @Test
    public void batchSizeLimitsFlowFilesPerRun() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("PROJ_A"));
        TestRunner runner = runner(scp.port(), "PROJ_A");
        runner.setProperty(StoreScu.BATCH_SIZE, "3");
        for (int i = 0; i < 7; i++) {
            runner.enqueue(TestDicom.ct(256).bytes());
        }

        runner.run(1);

        runner.assertTransferCount(StoreScu.REL_SUCCESS, 3);
        runner.assertQueueNotEmpty();
    }

    @Test
    public void failureStatusRoutesOnlyThatFlowFile() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("PROJ_A"));
        TestDicom.Instance ok1 = TestDicom.ct(256);
        TestDicom.Instance rejected = TestDicom.ct(256);
        TestDicom.Instance ok2 = TestDicom.ct(256);
        scp.failWith(rejected.iuid(), 0xA700);
        TestRunner runner = runner(scp.port(), "PROJ_A");
        runner.enqueue(ok1.bytes());
        runner.enqueue(rejected.bytes());
        runner.enqueue(ok2.bytes());

        runner.run();

        runner.assertTransferCount(StoreScu.REL_SUCCESS, 2);
        runner.assertTransferCount(StoreScu.REL_FAILURE, 1);
        MockFlowFile failed = runner.getFlowFilesForRelationship(StoreScu.REL_FAILURE).get(0);
        failed.assertAttributeEquals(StoreScu.STORE_ERROR_CODE, DicomStoreBatch.STORE_REJECTED);
        failed.assertAttributeEquals(StoreScu.STORE_ERROR_TYPE, DicomStoreBatch.STORE_REJECTED);
        failed.assertAttributeEquals(StoreScu.STORE_STATUS, "A700");
        assertTrue(failed.getAttribute(StoreScu.STORE_ERROR).contains("A700"));
        assertTrue(failed.isPenalized());
        failed.assertContentEquals(rejected.bytes());
        assertEquals(2, scp.received().size());
    }

    @Test
    public void destinationPerFlowFileViaExpressionLanguage() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("*"));
        TestRunner runner = runner(scp.port(), "${dcma.dest.aet}");
        runner.enqueue(TestDicom.ct(256).bytes(), Map.of("dcma.dest.aet", "PROJ_A"));
        runner.enqueue(TestDicom.ct(256).bytes(), Map.of("dcma.dest.aet", "PROJ_B"));
        runner.enqueue(TestDicom.ct(256).bytes(), Map.of("dcma.dest.aet", "PROJ_A"));

        runner.run();

        runner.assertAllFlowFilesTransferred(StoreScu.REL_SUCCESS, 3);
        assertEquals(2, scp.associationCount(), "one association per Called AE");
        assertEquals(2, scp.received().stream().filter(r -> r.calledAet().equals("PROJ_A")).count());
        assertEquals(1, scp.received().stream().filter(r -> r.calledAet().equals("PROJ_B")).count());
    }

    @Test
    public void invalidPortAttributeFailsOnlyThatFlowFile() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("PROJ_A"));
        TestRunner runner = runner(scp.port(), "PROJ_A");
        runner.setProperty(StoreScu.PORT, "${dcma.dest.port}");
        runner.enqueue(TestDicom.ct(256).bytes(), Map.of("dcma.dest.port", "not-a-port"));
        runner.enqueue(TestDicom.ct(256).bytes(), Map.of("dcma.dest.port", String.valueOf(scp.port())));

        runner.run();

        runner.assertTransferCount(StoreScu.REL_SUCCESS, 1);
        runner.assertTransferCount(StoreScu.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(StoreScu.REL_FAILURE).get(0)
                .assertAttributeEquals(StoreScu.STORE_ERROR_CODE, StoreScu.CONFIG_INVALID);
    }

    @Test
    public void unreachableScpFailsAllWithConnectionError() throws Exception {
        TestStoreScp scp = TestStoreScp.acceptingAll("PROJ_A");
        int port = scp.port();
        scp.close();
        TestRunner runner = runner(port, "PROJ_A");
        runner.enqueue(TestDicom.ct(256).bytes());
        runner.enqueue(TestDicom.ct(256).bytes());

        runner.run();

        runner.assertAllFlowFilesTransferred(StoreScu.REL_FAILURE, 2);
        for (MockFlowFile flowFile : runner.getFlowFilesForRelationship(StoreScu.REL_FAILURE)) {
            flowFile.assertAttributeEquals(StoreScu.STORE_ERROR_CODE, DicomStoreBatch.CONNECTION_FAILED);
            flowFile.assertAttributeNotExists(StoreScu.STORE_STATUS);
        }
    }

    @Test
    public void invalidContentFailsWithInvalidDicom() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("PROJ_A"));
        TestRunner runner = runner(scp.port(), "PROJ_A");
        runner.enqueue("no dicom".getBytes());

        runner.run();

        runner.assertAllFlowFilesTransferred(StoreScu.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(StoreScu.REL_FAILURE).get(0)
                .assertAttributeEquals(StoreScu.STORE_ERROR_CODE, DicomStoreBatch.INVALID_DICOM);
    }

    /** Content as GetScu and Pseudonymizer2 write it: data set without file meta information. */
    @Test
    public void bareDataSetsFromTheFlowAreStored() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("PROJ_A"));
        TestRunner runner = runner(scp.port(), "PROJ_A");
        TestDicom.Instance explicit = TestDicom.bare(UID.ExplicitVRLittleEndian, 256);
        TestDicom.Instance compressed = TestDicom.bareEncapsulated(128);
        runner.enqueue(explicit.bytes(), Map.of(StoreScu.TRANSFER_SYNTAX, UID.ImplicitVRLittleEndian));
        runner.enqueue(compressed.bytes(), Map.of(StoreScu.TRANSFER_SYNTAX, UID.JPEGBaseline8Bit));

        runner.run();

        runner.assertAllFlowFilesTransferred(StoreScu.REL_SUCCESS, 2);
        assertEquals(2, scp.received().size());
        // Native data in its own (pseudonymized) encoding, compressed data per GetScu attribute
        assertEquals(UID.ExplicitVRLittleEndian, scp.received().get(0).tsuid());
        assertEquals(UID.JPEGBaseline8Bit, scp.received().get(1).tsuid());
    }

    @Test
    public void bareCompressedDataSetWithoutTransferSyntaxAttributeFails() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("PROJ_A"));
        TestRunner runner = runner(scp.port(), "PROJ_A");
        runner.enqueue(TestDicom.bareEncapsulated(128).bytes());

        runner.run();

        runner.assertAllFlowFilesTransferred(StoreScu.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(StoreScu.REL_FAILURE).get(0)
                .assertAttributeEquals(StoreScu.STORE_ERROR_CODE, DicomStoreBatch.INVALID_DICOM);
        assertEquals(0, scp.received().size());
    }

    @Test
    public void successAfterRetryRemovesOldErrorAttributes() throws Exception {
        TestStoreScp scp = scp(TestStoreScp.acceptingAll("PROJ_A"));
        TestRunner runner = runner(scp.port(), "PROJ_A");
        runner.enqueue(TestDicom.ct(256).bytes(), Map.of(
                StoreScu.STORE_ERROR_CODE, DicomStoreBatch.CONNECTION_FAILED,
                StoreScu.STORE_ERROR, "earlier attempt",
                StoreScu.STORE_ERROR_TYPE, DicomStoreBatch.CONNECTION_FAILED));

        runner.run();

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(StoreScu.REL_SUCCESS).get(0);
        flowFile.assertAttributeNotExists(StoreScu.STORE_ERROR_CODE);
        flowFile.assertAttributeNotExists(StoreScu.STORE_ERROR);
        flowFile.assertAttributeNotExists(StoreScu.STORE_ERROR_TYPE);
        flowFile.assertAttributeEquals(StoreScu.STORE_STATUS, "0000");
    }
}
