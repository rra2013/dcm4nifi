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
public class MaskPxDataTest {
    private static List<byte[]> dcmObjectsIVRLE = new ArrayList<>();
    private static List<byte[]> dcmObjectsJPLL = new ArrayList<>();
    private TestRunner testRunner;

    @BeforeAll
    public static void readData() {
        //Get DICOM Files
        dcmObjectsIVRLE = DataForTest.DCMOBJECTS_IVRLE;
        dcmObjectsJPLL = DataForTest.DCMOBJECTS_JPLL;
        Assertions.assertTrue(dcmObjectsIVRLE.size() > 0);
    }
    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(MaskPixelData.class);
    }

    @Test
    public void testProcessor() {
        log.info("Begin Mask Pixel Test Processor Test");
        testRunner.setValidateExpressionUsage(false);
        //JPLL
        testRunner.setProperty(MaskPixelData.REGIONS, "[0,0,100,200],[112,0,100,200]");
        testRunner.setProperty(MaskPixelData.COLOR, "0");
        dcmObjectsIVRLE.forEach(dcmFileArray -> {
            HashMap<String, String> attr = new HashMap<>();
            testRunner.enqueue(dcmFileArray, attr);
            testRunner.run();
        });
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(MaskPixelData.REL_SUCCESS);
        log.info("Success: {}", success.size());
        testRunner.assertAllFlowFilesTransferred(MaskPixelData.REL_SUCCESS);

        testRunner.clearTransferState();
        //JPLL
        dcmObjectsJPLL.forEach(dcmFileArray -> {
            HashMap<String, String> attr = new HashMap<>();
            testRunner.enqueue(dcmFileArray, attr);
            testRunner.run();
        });
        List<MockFlowFile> errors = testRunner.getFlowFilesForRelationship(MaskPixelData.REL_FAILURE);
        log.info("Failure: {}", errors.size());
        testRunner.assertAllFlowFilesTransferred(MaskPixelData.REL_FAILURE);

    }

}
