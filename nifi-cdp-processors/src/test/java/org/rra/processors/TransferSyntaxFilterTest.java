package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcm4che3.data.UID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rra.processors.Utils.FileInfo;

import java.util.*;

import static org.rra.processors.TransfersyntaxFilter.*;

@Slf4j
public class TransferSyntaxFilterTest {
    private TestRunner testRunner;
    private static Map<FileInfo, byte[]> dcmObjects_ivrle = new HashMap<>();

    @BeforeAll
    public static void readData() {
        //Get DICOM Files
        dcmObjects_ivrle = DataForTest.DCMOBJECTS_UNCOMPRESSED;
        Assertions.assertTrue(dcmObjects_ivrle.size() > 0);
    }
    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(TransfersyntaxFilter.class);
    }

    @Test
    public void testProcessorSuccess() {
        log.info("Begin Transfer syntax Filter Processor Test");
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(OBJECT_TYPE, UNCOMPRESSED);

        //RUN 1
        Set<FileInfo> keys = dcmObjects_ivrle.keySet();
        keys.forEach(fileInfo -> {
            HashMap<String, String> attr = new HashMap<>();
            attr.put("TransferSyntax", fileInfo.getTransferSyntax());
            testRunner.enqueue("Test Data", attr);
            testRunner.run();
        });
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        List<MockFlowFile> error = testRunner.getFlowFilesForRelationship(REL_FAILURE);
        log.info("Success: {}", success.size());
        log.info("Error: {}", error.size());

        //RUN 2
        testRunner.setProperty(OBJECT_TYPE, VALUE);
        testRunner.setProperty(TRANSFER_SYNTAX, "*");
        keys.forEach(fileInfo -> {
            HashMap<String, String> attr = new HashMap<>();
            attr.put("TransferSyntax", fileInfo.getTransferSyntax());
            testRunner.enqueue("Test Data", attr);
            testRunner.run();
        });
        success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        error = testRunner.getFlowFilesForRelationship(REL_FAILURE);
        log.info("Success: {}", success.size());
        log.info("Error: {}", error.size());

        //RUN 3
        testRunner.setProperty(OBJECT_TYPE, VALUE);
        testRunner.setProperty(TRANSFER_SYNTAX, "1.2.840.10008.1.2");
        keys.forEach(fileInfo -> {
            HashMap<String, String> attr = new HashMap<>();
            attr.put("TransferSyntax", fileInfo.getTransferSyntax());
            testRunner.enqueue("Test Data", attr);
            testRunner.run();
        });
        success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        error = testRunner.getFlowFilesForRelationship(REL_FAILURE);
        log.info("Success: {}", success.size());
        log.info("Error: {}", error.size());

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 3 * keys.size());
    }
    @Test
    public void testProcessorFailed() {
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(OBJECT_TYPE, UNCOMPRESSED);

        //RUN 1
        Set<FileInfo> keys = dcmObjects_ivrle.keySet();
        keys.forEach(fileInfo -> {
            HashMap<String, String> attr = new HashMap<>();
            attr.put("TransferSyntax", fileInfo.getTransferSyntax()+".99");
            testRunner.enqueue("Test Data", attr);
            testRunner.run();
        });
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        List<MockFlowFile> error = testRunner.getFlowFilesForRelationship(REL_FAILURE);
        log.info("Success: {}", success.size());
        log.info("Error: {}", error.size());

        //RUN 2
        testRunner.setProperty(OBJECT_TYPE, VALUE);
        testRunner.setProperty(TRANSFER_SYNTAX, UID.ExplicitVRLittleEndian);
        keys = dcmObjects_ivrle.keySet();
        keys.forEach(fileInfo -> {
            HashMap<String, String> attr = new HashMap<>();
            attr.put("TransferSyntax", fileInfo.getTransferSyntax()+".99");
            testRunner.enqueue("Test Data", attr);
            testRunner.run();
        });
        success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        error = testRunner.getFlowFilesForRelationship(REL_FAILURE);
        log.info("Success: {}", success.size());
        log.info("Error: {}", error.size());

        testRunner.assertAllFlowFilesTransferred(REL_FAILURE, 2 * keys.size());

    }

}
