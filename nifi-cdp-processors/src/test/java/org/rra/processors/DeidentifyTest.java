package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Sequence;
import org.dcm4che3.data.Tag;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rra.dcm.DicomDataReader;
import org.rra.deidentify.model.DeidentifyModel;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


@Slf4j
public class DeidentifyTest {

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
        testRunner = TestRunners.newTestRunner(Deidentify.class);
    }

    @Test
    public void testModel() {
        DeidentifyModel model = null;
        try {
            model = DeidentifyModel.getModel();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Assertions.assertNotNull(model);
    }


    @Test
    public void testProcessor() {
        log.info("Begin De-Identify Processor Test");
        dcmObjects.forEach(dcmFileArray -> {
            HashMap<String, String> attr = new HashMap<>();
            attr.put("CallingAET", "TEST_RUNNER");
            testRunner.enqueue(dcmFileArray, attr);
            testRunner.run();
            log.info("Run with size {}", dcmFileArray.length);

        });
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(Deidentify.REL_SUCCESS);
        success.forEach(mockFlowFile -> {
            byte[] readAnonym = mockFlowFile.toByteArray();
            try (ByteArrayInputStream ba = new ByteArrayInputStream(readAnonym)) {
                try (BufferedInputStream bif = new BufferedInputStream(ba)) {
                    DicomDataReader data = new DicomDataReader(bif, true);
                    Attributes dcm = data.getAttributes();
                    log.info(" + + + SOPInstanceUID: {}", dcm.getString(Tag.SOPInstanceUID));
                    Sequence sequence = dcm.getSequence(Tag.DeidentificationMethodCodeSequence);
                    Assertions.assertNotNull(sequence);
                    log.debug("SEQ Size: {}", sequence.size());
                    //sequence.forEach(attributes -> log.info("SEQ: {}", attributes));
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        });
        log.info("Test De-Identify Processor OK. {} Files were de-identified.", success.size());
        List<MockFlowFile> error = testRunner.getFlowFilesForRelationship(Deidentify.REL_FAILURE);
        error.forEach(mockFlowFile -> {
            byte[] readAnonym = mockFlowFile.toByteArray();
            try (ByteArrayInputStream ba = new ByteArrayInputStream(readAnonym)) {
                try (BufferedInputStream bif = new BufferedInputStream(ba)) {
                    DicomDataReader data = new DicomDataReader(bif, true);
                    Attributes dcm = data.getAttributes();
                    log.info(" + + + SOPInstanceUID: {}", dcm.getString(Tag.SOPInstanceUID));
                    Sequence sequence = dcm.getSequence(Tag.DeidentificationMethodCodeSequence);
                    Assertions.assertNotNull(sequence);
                    log.debug("SEQ Size: {}", sequence.size());
                    //sequence.forEach(attributes -> log.info("SEQ: {}", attributes));
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        });
        log.info("Test De-Identify Processor Error. {} Files were NOT de-identified.", error.size());
        //assertions
        testRunner.assertAllFlowFilesTransferred(Deidentify.REL_SUCCESS, dcmObjects.size());
    }


}
