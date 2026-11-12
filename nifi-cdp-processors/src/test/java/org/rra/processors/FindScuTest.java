package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rra.cfind.NifiFindScu;
import org.rra.dcmconfig.DcmConfig;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.rra.processors.DataForTest.*;
import static org.rra.processors.FindScu.*;

@Slf4j
public class FindScuTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(FindScu.class);
    }

    @Test
    public void testProcessorSuccess() {
        if (!DICOM_INTEGRATION_TESTS){
            log.info("Skipping test because DICOM INTEGRATION_TESTS");
            return;
        }
        log.info("$ $ $ $ Run FindScu $ $ $ $ $");
        testRunner.setValidateExpressionUsage(false);

        Map<String, String> attributes = Map.of(
                "project.id", "CAIRO",
                "order.id", "test-order-123",
                "patient.id", "56757"
        );
        //Patient/study level
        testRunner.setProperty(FindScu.QUERY_LEVEL, PATSTUDY_LEVEL);
        testRunner.setProperty(REMOTE_HOST, DICOM_SERVER_HOST);
        testRunner.setProperty(PORT, Integer.toString(DICOM_SERVER_PORT));

        testRunner.enqueue(
                "56757".getBytes(StandardCharsets.UTF_8),
                attributes
        );

        testRunner.run();
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        log.info("Size of Success: {}", success.size());
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);//count of studies
        for (MockFlowFile result : success) {
            result.assertAttributeEquals("project.id", "CAIRO");
            result.assertAttributeEquals("order.id", "test-order-123");
            result.assertAttributeEquals("patient.id", "56757");
            result.assertAttributeEquals("mime.type", "application/dicom");
        }

        testRunner.clearTransferState();

        // Series Level with only StudyIUID as parameter
        testRunner.setProperty(FindScu.QUERY_LEVEL, SERIES_LEVEL);
        testRunner.enqueue("1.2.840.113845.11.1000000001900555490.20160718102042.2434233");// study rra fuss
        testRunner.run();
        success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        log.info("Size of Success: {}", success.size());
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS,14);
        testRunner.clearTransferState();


        // Series Level with (StudyIUID,SeriesIUID) as parameter
        testRunner.setProperty(FindScu.QUERY_LEVEL, SERIES_LEVEL);
        //StudyIUID,SeriesIUID
        testRunner.enqueue("1.2.840.113845.11.1000000001900555490.20160718102042.2434233,1.3.12.2.1107.5.2.19.45819.2016071811063980705662155.0.0.0");// study,series rra fuss
        testRunner.run();
        success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        log.info("Size of Success: {}", success.size());
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS,1);
        testRunner.clearTransferState();

        //Image level
        testRunner.setProperty(FindScu.QUERY_LEVEL, IMAGE_LEVEL);
        //StudyIUID and query on image level
        testRunner.enqueue("1.2.840.113845.11.1000000001900555490.20160718102042.2434233,1.3.12.2.1107.5.2.19.45819.2016071811120879334462944.0.0.0");
        testRunner.run();
        success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        log.info("Size of Success: {}", success.size());
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS,35);

    }

    @Test
    public void testProcessorFailed(){
        if (!DICOM_INTEGRATION_TESTS){
            log.info("Skipping test because DICOM INTEGRATION_TESTS");
            return;
        }
        log.info("$ $ $ $ Run FindScu $ $ $ $ $");
        testRunner.setValidateExpressionUsage(false);
        //Error test
        testRunner.setProperty(FindScu.QUERY_LEVEL, PATSTUDY_LEVEL);
        testRunner.setProperty(REMOTE_HOST, "wrong_host");
        testRunner.enqueue("56757"); //PID
        testRunner.run();
        List<MockFlowFile> failed = testRunner.getFlowFilesForRelationship(REL_FAILURE);
        log.info("Size of failed: {}", failed.size());
        testRunner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    // ------------------------------------------------------------------
    // Routing without a DICOM server: the query itself is stubbed.
    // ------------------------------------------------------------------

    /** FindScu whose C-FIND returns fixed matches or throws. */
    static class StubFindScu extends FindScu {
        private final List<Attributes> matches;
        private final Exception error;

        StubFindScu(List<Attributes> matches, Exception error) {
            this.matches = matches;
            this.error = error;
        }

        @Override
        protected List<Attributes> executeQuery(NifiFindScu findSCU) throws Exception {
            if (error != null) {
                throw error;
            }
            return matches;
        }
    }

    private static final String STUDY = "1.2.826.0.1.3680043.2.1";
    private static final String SERIES = "1.2.826.0.1.3680043.2.1.1";

    private static Attributes seriesMatch(String seriesUid) {
        Attributes attributes = new Attributes();
        attributes.setString(Tag.StudyInstanceUID, VR.UI, STUDY);
        attributes.setString(Tag.SeriesInstanceUID, VR.UI, seriesUid);
        return attributes;
    }

    private static TestRunner seriesRunner(FindScu processor) {
        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(FindScu.QUERY_LEVEL, SERIES_LEVEL);
        runner.setProperty(REMOTE_HOST, "localhost");
        runner.enqueue((STUDY + "," + SERIES).getBytes(StandardCharsets.UTF_8),
                Map.of("dcma.order.id", "order-1", "dcma.series.uid", SERIES,
                        FIND_ERROR, "error of an earlier attempt"));
        return runner;
    }

    @Test
    public void routesQueryWithoutMatchToNotFound() {
        TestRunner runner = seriesRunner(new StubFindScu(List.of(), null));

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_NOT_FOUND, 1);
        MockFlowFile original = runner.getFlowFilesForRelationship(REL_NOT_FOUND).get(0);
        original.assertContentEquals(STUDY + "," + SERIES);
        original.assertAttributeEquals(RESULT_COUNT, "0");
        original.assertAttributeEquals("dcma.order.id", "order-1");
        original.assertAttributeEquals("dcma.series.uid", SERIES);
    }

    @Test
    public void createsOneFlowFilePerMatchWithCountAndIndex() {
        TestRunner runner = seriesRunner(new StubFindScu(
                List.of(seriesMatch(SERIES), seriesMatch(SERIES + ".2")), null));

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);
        List<MockFlowFile> matches = runner.getFlowFilesForRelationship(REL_SUCCESS);
        for (int i = 0; i < matches.size(); i++) {
            matches.get(i).assertAttributeEquals(RESULT_COUNT, "2");
            matches.get(i).assertAttributeEquals(RESULT_INDEX, String.valueOf(i));
            matches.get(i).assertAttributeEquals("mime.type", "application/dicom");
            matches.get(i).assertAttributeEquals("dcma.order.id", "order-1");
            matches.get(i).assertAttributeNotExists(FIND_ERROR);
        }
    }

    @Test
    public void passesNumberOfSeriesRelatedInstancesOn() {
        Attributes withCount = seriesMatch(SERIES);
        withCount.setInt(Tag.NumberOfSeriesRelatedInstances, VR.IS, 5240);
        Attributes withoutCount = seriesMatch(SERIES + ".2");
        TestRunner runner = seriesRunner(new StubFindScu(List.of(withCount, withoutCount), null));

        runner.run();

        List<MockFlowFile> matches = runner.getFlowFilesForRelationship(REL_SUCCESS);
        matches.get(0).assertAttributeEquals(SERIES_INSTANCES, "5240");
        matches.get(1).assertAttributeNotExists(SERIES_INSTANCES);
    }

    @Test
    public void routesQueryErrorToFailureWithMessage() {
        TestRunner runner = seriesRunner(new StubFindScu(
                null, new java.io.IOException("C-FIND final status: 0xA700")));

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(REL_FAILURE).get(0)
                .assertAttributeEquals(FIND_ERROR, "C-FIND final status: 0xA700");
    }

    @Test
    public void seriesLevelWithoutStudyUidFails() {
        TestRunner runner = TestRunners.newTestRunner(new StubFindScu(List.of(), null));
        runner.setValidateExpressionUsage(false);
        runner.setProperty(FindScu.QUERY_LEVEL, SERIES_LEVEL);
        runner.setProperty(REMOTE_HOST, "localhost");
        runner.enqueue(" ,".getBytes(StandardCharsets.UTF_8));

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    @Test
    public void testSCUPatStudy() throws Exception {
        if (!DICOM_INTEGRATION_TESTS){
            log.info("Skipping test because DICOM INTEGRATION_TESTS");
            return;
        }
        DcmConfig cfg = new DcmConfig();
        cfg.FIND_LEVEL = DcmConfig.LEVEL.STUDY;

        NifiFindScu findSCU = new NifiFindScu("FIND", DICOM_SERVER_AET, DICOM_SERVER_HOST, DICOM_SERVER_PORT, cfg);
        //Pat/Study level
        final List<Attributes> resultSet =
                Collections.synchronizedList(new ArrayList<>());
        findSCU.getQueryFilter().setPatientID("56757");
        findSCU.getQueryFilter().setIssuerOfPatientID("HANAU");
        findSCU.doQuery(remote -> {
            log.info("Concected to host:{}", remote.getHostname());
        },data -> {
            resultSet.add(data);
            log.info("Data -> \n{}", data);
        });
        Assertions.assertEquals(1, resultSet.size());


        findSCU = new NifiFindScu("FIND", DICOM_SERVER_AET, DICOM_SERVER_HOST, DICOM_SERVER_PORT, cfg);
        //Pat/Study level
        final List<Attributes> res = new ArrayList<>();
        findSCU.getQueryFilter().setPatientID("56757");
        findSCU.getQueryFilter().setIssuerOfPatientID("HANAU");
        findSCU.getQueryFilter().setStudyInstanceUID("1.2.840.113845.11.1000000001900555490.20160718102042.2434233");
        findSCU.doQuery(remote -> {
            log.info("Concected to host:{}", remote.getHostname());
        },data -> {
            res.add(data);
            log.info("Data -> \n{}", data);
        });
        Assertions.assertEquals(1, res.size());
    }
    @Test
    public void testSeriesLevel() throws Exception {
        if (!DICOM_INTEGRATION_TESTS){
            log.info("Skipping test because DICOM INTEGRATION_TESTS");
            return;
        }
        DcmConfig cfg = new DcmConfig();
        cfg.FIND_LEVEL = DcmConfig.LEVEL.SERIES;
        NifiFindScu findSCU = new NifiFindScu("FIND", DICOM_SERVER_AET, DICOM_SERVER_HOST, DICOM_SERVER_PORT, cfg);
        //Series level
        final List<Attributes> resultSet = new ArrayList<>();
        findSCU.getQueryFilter().setPatientID("56757");
        findSCU.getQueryFilter().setIssuerOfPatientID("HANAU");
        findSCU.getQueryFilter().setStudyInstanceUID("1.2.840.113845.11.1000000001900555490.20160718102042.2434233");
        findSCU.doQuery(remote -> {
            log.info("Concected to host:{}", remote.getHostname());
        },data -> {
            resultSet.add(data);
            log.info("Data -> \n{}", data);
        });
        Assertions.assertEquals(14, resultSet.size());
        resultSet.clear();

        findSCU = new NifiFindScu(
                "FIND",
                DICOM_SERVER_AET,
                DICOM_SERVER_HOST,
                DICOM_SERVER_PORT,
                cfg
        );

        findSCU.getQueryFilter().setPatientID("56757");
        findSCU.getQueryFilter().setIssuerOfPatientID("HANAU");
        findSCU.getQueryFilter().setStudyInstanceUID("1.2.840.113845.11.1000000001900555490.20160718102042.2434233");
        findSCU.getQueryFilter().setSeriesInstanceUID("1.3.12.2.1107.5.2.19.45819.2016071811063980705662155.0.0.0");
        findSCU.doQuery(remote -> {
            log.info("Concected to host:{}", remote.getHostname());
        },data -> {
            resultSet.add(data);
            log.info("Data -> \n{}", data);
        });
        Assertions.assertEquals(1, resultSet.size());
    }

    @Test
    public void testImageLevel() throws Exception {
        if (!DICOM_INTEGRATION_TESTS){
            log.info("Skipping test because DICOM INTEGRATION_TESTS");
            return;
        }
        DcmConfig cfg = new DcmConfig();
        cfg.FIND_LEVEL = DcmConfig.LEVEL.IMAGE;
        NifiFindScu findSCU = new NifiFindScu("FIND", DICOM_SERVER_AET, DICOM_SERVER_HOST, DICOM_SERVER_PORT, cfg);
        //Image level
        final List<Attributes> resultSet = new ArrayList<>();
        findSCU.getQueryFilter().setPatientID("56757");
        findSCU.getQueryFilter().setIssuerOfPatientID("HANAU");
        findSCU.getQueryFilter().setStudyInstanceUID("1.2.840.113845.11.1000000001900555490.20160718102042.2434233");
        findSCU.doQuery(remote -> {
            log.info("Concected to host:{}", remote.getHostname());
        },data -> {
            resultSet.add(data);
            log.info("Data -> \n{}", data);
        });
        Assertions.assertEquals(300, resultSet.size());

        // For a series inside a study
        findSCU = new NifiFindScu("FIND", DICOM_SERVER_AET, DICOM_SERVER_HOST, DICOM_SERVER_PORT, cfg);
        //Image level
        final List<Attributes> res = new ArrayList<>();
        findSCU.getQueryFilter().setPatientID("56757");
        findSCU.getQueryFilter().setIssuerOfPatientID("HANAU");
        findSCU.getQueryFilter().setStudyInstanceUID("1.2.840.113845.11.1000000001900555490.20160718102042.2434233");
        findSCU.getQueryFilter().setSeriesInstanceUID("1.3.12.2.1107.5.2.19.45819.2016071811120879334462944.0.0.0");
        findSCU.doQuery(remote -> {
            log.info("Concected to host:{}", remote.getHostname());
        },data -> {
            res.add(data);
            log.info("Data -> \n{}", data);
        });
        Assertions.assertEquals(35, res.size());
    }


}
