package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class Dcm2JsonTest {
    private static List<byte[]> dcmObjects = new ArrayList<>();
    private TestRunner testRunner;

    @BeforeAll
    public static void readData() {
        //Get DICOM Files
        dcmObjects = DataForTest.DCMOBJECTS;
        Assertions.assertTrue(dcmObjects.size() > 0);
    }

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(Dcm2Json.class);
    }

    @Test
    public void testProcessorSuccess() {
        log.info("$ $ $ $ Run DCM2JSON $ $ $ $ $");
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(Dcm2Json.BULK_DATA, Dcm2Json.INCLUDE_BULK_DATA);
        testRunner.setProperty(Dcm2Json.INDENT_JSON, "true");
        testRunner.setProperty(Dcm2Json.ENCODE_AS_NUMBER, "true");

        dcmObjects.forEach(dcmFileArray -> {
            testRunner.enqueue(dcmFileArray);
            testRunner.run();
            log.info("Run with size {}", dcmFileArray.length);
        });
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(Dcm2Json.REL_SUCCESS);
        log.info("Size of success: {}", success.size());

        testRunner.setProperty(Dcm2Json.INDENT_JSON, "false");
        testRunner.setProperty(Dcm2Json.ENCODE_AS_NUMBER, "false");
        testRunner.setProperty(Dcm2Json.BULK_DATA, Dcm2Json.NO_BULK_DATA);
        dcmObjects.forEach(dcmFileArray -> {
            testRunner.enqueue(dcmFileArray);
            testRunner.run();
            log.info("Run with size {}", dcmFileArray.length);
        });

        testRunner.assertAllFlowFilesTransferred(Dcm2Json.REL_SUCCESS);
    }
}
