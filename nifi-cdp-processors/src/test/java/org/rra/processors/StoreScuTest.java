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
public class StoreScuTest {

    private static final List<byte[]> dcmObjects = new ArrayList<>();

    private TestRunner testSCU;
    private TestRunner testSCP;
    private StoreScp proc;
    @BeforeAll
    public static void readData() {
        //Read DICOM Files
        Utils.readDicomFiles(dcmObjects, DataForTest.DICOM_PATH);
        Assertions.assertTrue(dcmObjects.size() > 0);
    }
    @BeforeEach
    public void init() {
        proc = new StoreScp();
        testSCU = TestRunners.newTestRunner(StoreScu.class);
        testSCP = TestRunners.newTestRunner(proc);
    }
    @Test
    public void testProcessor() {
        testSCU.setValidateExpressionUsage(false);
        testSCP.setValidateExpressionUsage(false);
        //
        testSCP.setProperty(StoreScp.PORT, "11112");
        testSCP.enqueue("TEST");
        testSCP.run(1, false, true);
        log.info("$ $ $ $ Run testSCP $ $ $ $ $");
        dcmObjects.forEach(dcmFileArray -> {
            HashMap<String, String> attr = new HashMap<>();
            attr.put("CallingAET", "TEST_RUNNER");
            testSCU.setProperty(StoreScu.PORT, "11112");
            testSCU.enqueue(dcmFileArray, attr);
            testSCU.run();
            log.info("Run with size {}", dcmFileArray.length);
        });

        List<MockFlowFile> success = testSCU.getFlowFilesForRelationship(StoreScu.REL_SUCCESS);
        List<MockFlowFile> successSCP = testSCP.getFlowFilesForRelationship(StoreScp.REL_SUCCESS);
        log.info("Count of success {}:{}", success.size(), successSCP.size());
        Assertions.assertTrue(success.size() >= dcmObjects.size());
    }

}
