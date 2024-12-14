package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rra.dcm.DicomDataReader;
import org.rra.store.StoreSCU;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class StoreScpProcessorTest {
    private static final String DICOM_PATH = "/home/dev/Documents/DICOM/AE801BA5";
    private static final List<byte[]> dcmObjects = new ArrayList<>();

    private TestRunner testRunner;
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
        testRunner = TestRunners.newTestRunner(proc);
    }


    @Test
    public void testProcessor() {
        testRunner.enqueue("TEST");
        testRunner.run(1, false, true);
        log.info("$ $ $ $ Run $ $ $ $ $");
        try {
            dcmObjects.forEach(bytes -> {
                try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes)) {
                    try (BufferedInputStream bis = new BufferedInputStream(byteArrayInputStream)) {
                        new StoreSCU("dev-test-vm1", 11115, "NIFI_SCU", "DCM4NIFI", bis);
                    }
                } catch (Exception e) {

                }
            });
            List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(StoreScpProcessor.REL_SUCCESS);
            log.info("Count of success {}", success.size());
            Assertions.assertTrue(success.size() >= dcmObjects.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void testData(){
        dcmObjects.forEach(bytes -> {
            try(ByteArrayInputStream ba = new ByteArrayInputStream(bytes)){
                try(BufferedInputStream bif = new BufferedInputStream(ba)){
                    DicomDataReader data = new DicomDataReader(bif, true);
                    Attributes dcm = data.getAttributes();
                    log.info(" + + + SOPInstanceUID: {}", dcm.getString(Tag.SOPInstanceUID));
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        });
    }
}
