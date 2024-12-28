package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcm4che3.data.Attributes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rra.cfind.NifiFindScu;

import java.util.ArrayList;
import java.util.List;

import static org.rra.cfind.NifiFindScu.*;
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
        //Patient/study level
        testRunner.setProperty(FindScu.QUERY_LEVEL, PATSTUDY_LEVEL);
        testRunner.setProperty(REMOTE_HOST, DICOM_SERVER_HOST);
        testRunner.setProperty(PORT, Integer.toString(DICOM_SERVER_PORT));
        testRunner.enqueue("56757");
        testRunner.run();
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        log.info("Size of Success: {}", success.size());
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        // Series Level
        testRunner.setProperty(FindScu.QUERY_LEVEL, SERIES_LEVEL);
        testRunner.enqueue("1.2.840.113845.11.1000000001900555490.20160718102042.2434233");
        testRunner.run();
        success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        log.info("Size of Success: {}", success.size());
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS,15);
        //Image level
        testRunner.setProperty(FindScu.QUERY_LEVEL, IMAGE_LEVEL);
        testRunner.enqueue("1.2.840.113845.11.1000000001900555490.20160718102042.2434233");
        testRunner.run();
        success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        log.info("Size of Success: {}", success.size());
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS,315);

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
        testRunner.enqueue("56757");
        testRunner.run();
        List<MockFlowFile> failed = testRunner.getFlowFilesForRelationship(REL_FAILURE);
        log.info("Size of failed: {}", failed.size());
        testRunner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    @Test
    public void testSCUPatStudy() throws Exception {
        if (!DICOM_INTEGRATION_TESTS){
            log.info("Skipping test because DICOM INTEGRATION_TESTS");
            return;
        }
        NifiFindScu findSCU = new NifiFindScu("FIND", DICOM_SERVER_AET, DICOM_SERVER_HOST, DICOM_SERVER_PORT, QUERY_LEVEL_PATIENT_STUDY);
        //Pat/Study level
        final List<Attributes> resultSet = new ArrayList<>();

        findSCU.getQueryFilter().setPatientID("56757");
        findSCU.getQueryFilter().setIssuerOfPatientID("HANAU");
        findSCU.doQuery(remote -> {
            log.info("Concected to host:{}", remote.getHostname());
        },data -> {
            resultSet.add(data);
            log.info("Data -> \n{}", data);
        });
        Assertions.assertEquals(1, resultSet.size());

        findSCU = new NifiFindScu("FIND", DICOM_SERVER_AET, DICOM_SERVER_HOST, DICOM_SERVER_PORT, QUERY_LEVEL_PATIENT_STUDY);
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
        NifiFindScu findSCU = new NifiFindScu("FIND", DICOM_SERVER_AET, DICOM_SERVER_HOST, DICOM_SERVER_PORT, QUERY_LEVEL_SERIES);
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
    }

    @Test
    public void testImageLevel() throws Exception {
        if (!DICOM_INTEGRATION_TESTS){
            log.info("Skipping test because DICOM INTEGRATION_TESTS");
            return;
        }
        NifiFindScu findSCU = new NifiFindScu("FIND", DICOM_SERVER_AET, DICOM_SERVER_HOST, DICOM_SERVER_PORT, QUERY_LEVEL_IMAGE);
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
        findSCU = new NifiFindScu("FIND", DICOM_SERVER_AET, DICOM_SERVER_HOST, DICOM_SERVER_PORT, QUERY_LEVEL_IMAGE);
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
