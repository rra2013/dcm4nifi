package org.rra.processors;

import ca.uhn.hl7v2.util.XMLUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rra.dcm.Dicom2XmlTransformer;
import org.rra.dcm.DicomUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Slf4j
public class Dcm2XmlTest {

    private static List<byte[]> dcmObjects = new ArrayList<>();
    private static List<byte[]> sr_dcmObjects = new ArrayList<>();
    @BeforeAll
    public static void readData() {
        //Get DICOM Files
        dcmObjects = DataForTest.DCMOBJECTS;
        sr_dcmObjects = DataForTest.SR_OBJECTS;
        Assertions.assertTrue(dcmObjects.size() > 0);
        Assertions.assertTrue(sr_dcmObjects.size() > 0);
    }

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(Dcm2Xml.class);
    }

    @Test
    public void testProcessorSuccess() {
        log.info("$ $ $ $ Run DCM2XML $ $ $ $ $");
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(Dcm2Xml.BULK_DATA, Dcm2Xml.INCLUDE_BULK_DATA);

        dcmObjects.forEach(dcmFileArray -> {
            testRunner.enqueue(dcmFileArray);
            testRunner.run();
            log.info("Run with size {}", dcmFileArray.length);
        });
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(Dcm2Xml.REL_SUCCESS);
        log.info("Size of success: {}", success.size());

        testRunner.setProperty(Dcm2Xml.BULK_DATA, Dcm2Xml.NO_BULK_DATA);
        dcmObjects.forEach(dcmFileArray -> {
            testRunner.enqueue(dcmFileArray);
            testRunner.run();
            log.info("Run with size {}", dcmFileArray.length);
        });

        testRunner.setProperty(Dcm2Xml.BULK_DATA, Dcm2Xml.NO_BULK_DATA);
        testRunner.setProperty(Dcm2Xml.XSL_TRANSFORM_PATH, "/opt/dcm4che/etc/dcm2xml/srdump.xsl");
        dcmObjects.forEach(dcmFileArray -> {
            testRunner.enqueue(dcmFileArray);
            testRunner.run();
            log.info("Run with size {}", dcmFileArray.length);
        });

        success.forEach(mockFlowFile -> {
            byte[] readAnonym = mockFlowFile.toByteArray();
            try(ByteArrayInputStream ba = new ByteArrayInputStream(readAnonym)){
                try(BufferedInputStream bif = new BufferedInputStream(ba)){
                    String xml = IOUtils.toString(bif, StandardCharsets.UTF_8);
                    log.info("size of Object: {}", xml.getBytes().length);
                    //System.out.println(xml);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        testRunner.assertAllFlowFilesTransferred(Dcm2Xml.REL_SUCCESS);
    }
    @Test
    public void testProcessorSuccessXSLT() {
        log.info("$ $ $ $ Run DCM2XML XSLT $ $ $ $ $");
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(Dcm2Xml.BULK_DATA, Dcm2Xml.NO_BULK_DATA);
        testRunner.setProperty(Dcm2Xml.XSL_TRANSFORM_PATH, "/opt/dcm4che/etc/dcm2xml/dsr2html.xsl");
        dcmObjects.forEach(dcmFileArray -> {
            testRunner.enqueue(dcmFileArray);
            testRunner.run();
            log.info("Run with size {}", dcmFileArray.length);
        });

        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(Dcm2Xml.REL_SUCCESS);
        success.forEach(mockFlowFile -> {
            byte[] readAnonym = mockFlowFile.toByteArray();
            try(ByteArrayInputStream ba = new ByteArrayInputStream(readAnonym)){
                try(BufferedInputStream bif = new BufferedInputStream(ba)){
                    String xml = IOUtils.toString(bif, StandardCharsets.UTF_8);
                    log.info("size of Object: {}", xml.getBytes().length);
                    //System.out.println(xml);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        testRunner.assertAllFlowFilesTransferred(Dcm2Xml.REL_SUCCESS);
    }
    @Test
    public void testXSLT() {
        final boolean inclBulk = false;
        String xslTransformPath = "/opt/dcm4che/etc/dcm2xml/dsr2html.xsl";
        sr_dcmObjects.forEach(dcmFileArray -> {
            try(ByteArrayInputStream ba = new ByteArrayInputStream(dcmFileArray)){
                try(BufferedInputStream bif = new BufferedInputStream(ba)){
                    try(ByteArrayOutputStream baot = new ByteArrayOutputStream()){
                        Dicom2XmlTransformer.transform(bif, baot, inclBulk, xslTransformPath);
                        InputStream inputStream = new ByteArrayInputStream(baot.toByteArray());
                        String html = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                        long size = html.getBytes(StandardCharsets.UTF_8).length;
                        //System.out.println("html = " + html);
                        Assertions.assertTrue(size > 0);
                    }catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        String xslTransformPath_dump = "/opt/dcm4che/etc/dcm2xml/srdump.xsl";
        sr_dcmObjects.forEach(dcmFileArray -> {
            try(ByteArrayInputStream ba = new ByteArrayInputStream(dcmFileArray)){
                try(BufferedInputStream bif = new BufferedInputStream(ba)){
                    try(ByteArrayOutputStream baot = new ByteArrayOutputStream()){
                        Dicom2XmlTransformer.transform(bif, baot, inclBulk, xslTransformPath_dump);
                        InputStream inputStream = new ByteArrayInputStream(baot.toByteArray());
                        String dump = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                        long size = dump.getBytes(StandardCharsets.UTF_8).length;
                        //System.out.println("Dump = " + dump);

                        
                        Assertions.assertTrue(size > 0);
                    }catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        boolean noBulk = false;
        dcmObjects.forEach(dcmFileArray -> {
            try(ByteArrayInputStream ba = new ByteArrayInputStream(dcmFileArray)){
                try(BufferedInputStream bif = new BufferedInputStream(ba)){
                    try(ByteArrayOutputStream baot = new ByteArrayOutputStream()){
                        Dicom2XmlTransformer.transform(bif, baot, noBulk, xslTransformPath);
                        InputStream inputStream = new ByteArrayInputStream(baot.toByteArray());
                        String html = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                        long size = html.getBytes(StandardCharsets.UTF_8).length;
                        //System.out.println("size html = " + size);
                        Assertions.assertTrue(size > 0);
                    }catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
