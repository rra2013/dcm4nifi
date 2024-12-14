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
@Slf4j
public class StoreScuProcessorTest {
    private static final String DICOM_PATH = "/home/dev/Documents/DICOM/2009";
    private static final List<byte[]> dcmObjects = new ArrayList<>();

    private TestRunner testRunner;
    private TestRunner testSCP;
    private StoreScpProcessor proc;
    @BeforeAll
    public static void readData() {
        //Read DICOM Files
        Utils.readDicomFiles(dcmObjects, DICOM_PATH);
        Assertions.assertTrue(dcmObjects.size() > 0);
    }
    @BeforeEach
    public void init() {
        proc = new StoreScpProcessor();
        testRunner = TestRunners.newTestRunner(StoreScuProcessor.class);
        testSCP = TestRunners.newTestRunner(proc);
    }
    @Test
    public void testProcessor() {
        testSCP.enqueue("TEST");
        testSCP.run(1, false, true);
        log.info("$ $ $ $ Run testSCP $ $ $ $ $");
        dcmObjects.forEach(dcmFileArray -> {
            HashMap<String, String> attr = new HashMap<>();
            attr.put("RetrieveAET", "TEST_RUNNER");
            testRunner.enqueue(dcmFileArray, attr);
            testRunner.run();
            log.info("Run with size {}", dcmFileArray.length);
        });

        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(StoreScuProcessor.REL_SUCCESS);
        List<MockFlowFile> successSCP = testSCP.getFlowFilesForRelationship(StoreScpProcessor.REL_SUCCESS);
        log.info("Count of success {}:{}", success.size(), successSCP.size());
        Assertions.assertTrue(success.size() >= dcmObjects.size());
    }

}
