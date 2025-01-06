package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rra.dcm.DcmObjectType;
import org.rra.dcm.DicomUtils;
import org.rra.dcm.SOPClassInfo;

import java.util.*;

@Slf4j
public class SOPClassFilterTest {
    private TestRunner testRunner;
    private static List<byte[]> dcmObjects = new ArrayList<>();
    @BeforeAll
    public static void readData() {
        //Get DICOM Files
        dcmObjects = DataForTest.DCMOBJECTS;
        Assertions.assertTrue(dcmObjects.size() > 0);
    }
    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(SOPClassFilter.class);
    }

    @Test
    public void testProcessorSuccess() {
        log.info("Begin SOP Class Filter Processor Test");
        testRunner.setValidateExpressionUsage(false);
        //Test Lauf All
        testRunner.setProperty(SOPClassFilter.OBJECT_TYPE, SOPClassFilter.VALUE);
        testRunner.setProperty(SOPClassFilter.FILTER_SOP_CLASS, "*");
        dcmObjects.forEach(bytes -> {
            Attributes dcm = DicomUtils.byteArrayToAttributes(bytes);
            String sopCUID = dcm.getString(Tag.SOPClassUID);
            HashMap<String, String> attr = new HashMap<>();
            attr.put("AffectedSOPClassUID", sopCUID);
            attr.put("TransferSyntax", "1.2.840.10008.1.2");
            testRunner.enqueue(bytes, attr);
            testRunner.run();
        });
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(SOPClassFilter.REL_SUCCESS);
        log.info("Success {}", success.size());
        //Test lauf 2
        testRunner.setProperty(SOPClassFilter.OBJECT_TYPE, SOPClassFilter.ALL);
        testRunner.setProperty(SOPClassFilter.FILTER_SOP_CLASS, "not_used");
        dcmObjects.forEach(bytes -> {
            Attributes dcm = DicomUtils.byteArrayToAttributes(bytes);
            String sopCUID = dcm.getString(Tag.SOPClassUID);
            HashMap<String, String> attr = new HashMap<>();
            attr.put("AffectedSOPClassUID", sopCUID);
            attr.put("TransferSyntax", "1.2.840.10008.1.2");
            testRunner.enqueue(bytes, attr);
            testRunner.run();
        });
        success = testRunner.getFlowFilesForRelationship(SOPClassFilter.REL_SUCCESS);
        log.info("Success {}", success.size());
        //test Lauf 3
        testRunner.setProperty(SOPClassFilter.OBJECT_TYPE, SOPClassFilter.UNCOMPRESSED_SINGLE_FRAME_IMAGE);
        testRunner.setProperty(SOPClassFilter.FILTER_SOP_CLASS, "not_used");
        dcmObjects.forEach(bytes -> {
            Attributes dcm = DicomUtils.byteArrayToAttributes(bytes);
            String sopCUID = dcm.getString(Tag.SOPClassUID);
            HashMap<String, String> attr = new HashMap<>();
            attr.put("AffectedSOPClassUID", sopCUID);
            attr.put("TransferSyntax", "1.2.840.10008.1.2");
            testRunner.enqueue(bytes, attr);
            testRunner.run();
        });
        success = testRunner.getFlowFilesForRelationship(SOPClassFilter.REL_SUCCESS);
        log.info("Success {}", success.size());


        testRunner.assertAllFlowFilesTransferred(SOPClassFilter.REL_SUCCESS, 3 * dcmObjects.size());
    }

    @Test
    public void testProcessorFailed() {
        testRunner.setValidateExpressionUsage(false);

        testRunner.setProperty(SOPClassFilter.OBJECT_TYPE, SOPClassFilter.COMPRESSED_SINGLE_FRAME_IMAGE);
        testRunner.setProperty(SOPClassFilter.FILTER_SOP_CLASS, "not_used");
        dcmObjects.forEach(bytes -> {
            Attributes dcm = DicomUtils.byteArrayToAttributes(bytes);
            String sopCUID = dcm.getString(Tag.SOPClassUID);
            HashMap<String, String> attr = new HashMap<>();
            attr.put("AffectedSOPClassUID", sopCUID);
            attr.put("TransferSyntax", "1.2.840.10008.1.2");
            testRunner.enqueue(bytes, attr);
            testRunner.run();
        });
        List<MockFlowFile> err = testRunner.getFlowFilesForRelationship(SOPClassFilter.REL_FAILURE);
        log.info("Error {}", err.size());
        // Lauf 2
        testRunner.setProperty(SOPClassFilter.OBJECT_TYPE, SOPClassFilter.UNCOMPRESSED_MULTI_FRAME_IMAGE);
        testRunner.setProperty(SOPClassFilter.FILTER_SOP_CLASS, "not_used");
        dcmObjects.forEach(bytes -> {
            Attributes dcm = DicomUtils.byteArrayToAttributes(bytes);
            String sopCUID = dcm.getString(Tag.SOPClassUID);
            HashMap<String, String> attr = new HashMap<>();
            attr.put("AffectedSOPClassUID", sopCUID);
            attr.put("TransferSyntax", "1.2.840.10008.1.2");
            testRunner.enqueue(bytes, attr);
            testRunner.run();
        });
        err = testRunner.getFlowFilesForRelationship(SOPClassFilter.REL_FAILURE);
        log.info("Error {}", err.size());
        testRunner.assertAllFlowFilesTransferred(SOPClassFilter.REL_FAILURE, 2 * dcmObjects.size());

    }
    @Test
    public void testObjectTypes(){
        Attributes attr = new Attributes();
        attr.setString(Tag.SOPClassUID, VR.UI, UID.BasicTextSRStorage);
        String tsUID = UID.ExplicitVRLittleEndian;
        SOPClassInfo sop = new SOPClassInfo(attr, tsUID);
        DcmObjectType dcmObjectType = DcmObjectType.objectTypeOf(sop);
        System.out.println("dcmObjectType = " + dcmObjectType.toString());
        Assertions.assertEquals("SRDocument", dcmObjectType.toString());
        //
        attr.setString(Tag.SOPClassUID, VR.UI, UID.EncapsulatedPDFStorage);
        sop = new SOPClassInfo(attr, tsUID);
        dcmObjectType = DcmObjectType.objectTypeOf(sop);
        System.out.println("dcmObjectType = " + dcmObjectType.toString());
        Assertions.assertEquals("EncapsulatedPDF", dcmObjectType.toString());

    }
}
