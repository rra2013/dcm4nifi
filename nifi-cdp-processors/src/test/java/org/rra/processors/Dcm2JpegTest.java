package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcm4che3.data.Attributes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rra.dcm.Dicom2JpegTransformer;
import org.rra.dcm.DicomUtils;

import javax.imageio.ImageIO;
import javax.imageio.stream.FileImageOutputStream;
import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.io.*;
import java.util.*;

import static org.rra.processors.Dcm2Jpeg.*;

@Slf4j
public class Dcm2JpegTest {
    private static Map<Utils.FileInfo, byte[]> dcmObjects = new HashMap<>();

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(Dcm2Jpeg.class);
    }

    @BeforeAll
    public static void readData() {
        //Get DICOM Files
        dcmObjects = DataForTest.DCMOBJECTS_UNCOMPRESSED;
        Assertions.assertTrue(dcmObjects.size() > 0);
    }

    @Test
    public void testDcm2JpegProcessor() {
        log.info("Begin Dcm2Jpeg Test");

        Set<Utils.FileInfo> keys = dcmObjects.keySet();
        keys.forEach(ts -> {
            byte[] bytes = dcmObjects.get(ts);
            HashMap<String, String> attr = new HashMap<>();
            attr.put("TransferSyntax", ts.getTransferSyntax());
            testRunner.enqueue(bytes, attr);
            testRunner.run();
        });


        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        log.info("Size of Success: {}", success.size());
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
    }

    //@Test
    public void testDcm2Jpeg() {
       /* dcmObjects.forEach(dcmBytes -> {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(dcmBytes)) {
                try (BufferedInputStream bis = new BufferedInputStream(bais)) {
                    try (OutputStream os = new ByteArrayOutputStream()) {
                        try (ImageOutputStream imageOutputStream = ImageIO.createImageOutputStream(os)) {
                            Dicom2JpegTransformer transformer = new Dicom2JpegTransformer();
                            transformer.transform(1, bis, imageOutputStream);
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });*/
    }
}
