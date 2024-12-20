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
import java.util.HashMap;
import java.util.List;

import static org.rra.processors.Dcm2Dcm.*;
import static org.rra.processors.Utils.readDicomFiles;

@Slf4j
public class Dcm2DcmTest {
    private static final List<byte[]> dcmObjects = new ArrayList<>();
    private TestRunner testRunner;

    @BeforeAll
    public static void readData() {
        //Read DICOM Files
        readDicomFiles(dcmObjects, DataForTest.DICOM_PATH);
        Assertions.assertTrue(dcmObjects.size() > 0);
    }
    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(Dcm2Dcm.class);
    }

    @Test
    public void testProcessor() {
        log.info("Begin Dcm2Dcm Processor Test");
        testRunner.setValidateExpressionUsage(false);
        //JPLL
        testRunner.setProperty(TRANSFER_SYNTAX, JPLL);
        dcmObjects.forEach(dcmFileArray -> {
            HashMap<String, String> attr = new HashMap<>();
            attr.put("TransferSyntax", "TEST_RUNNER from byte Array");
            testRunner.enqueue(dcmFileArray, attr);
            testRunner.run();
            log.info("Run with size {}", dcmFileArray.length);
        });
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        log.info("Success: {}", success.size());

        //JP2KR
        testRunner.setProperty(TRANSFER_SYNTAX, JP2KR);
        dcmObjects.forEach(dcmFileArray -> {
            HashMap<String, String> attr = new HashMap<>();
            attr.put("TransferSyntax", "TEST_RUNNER from byte Array");
            testRunner.enqueue(dcmFileArray, attr);
            testRunner.run();
            log.info("Run with size {}", dcmFileArray.length);
        });
        success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        log.info("Success: {}", success.size());

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
    }

}
