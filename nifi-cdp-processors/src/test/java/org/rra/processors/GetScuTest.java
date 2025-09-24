package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rra.cget.NifiGetScu;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.rra.dcm.DicomUtils.copyAttributesToOutput;
import static org.rra.processors.DataForTest.*;
import static org.rra.processors.GetScu.*;

@Slf4j
public class GetScuTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(GetScu.class);
    }

    @Test
    public void testProcessorSuccessStudyLevel() {

        if (!DICOM_INTEGRATION_TESTS) {
            log.info("Skipping test because DICOM INTEGRATION_TESTS");
            return;
        }

        log.info("$ $ $ $ Run GetSCU $ $ $ $ $");
        testRunner.setValidateExpressionUsage(false);
        //Patient/study level
        testRunner.setProperty(GET_LEVEL, STUDY_LEVEL);
        testRunner.setProperty(REMOTE_HOST, DICOM_SERVER_HOST);
        testRunner.setProperty(PORT, Integer.toString(DICOM_SERVER_PORT));
        final Attributes input = new Attributes();
        String studyInstanceUID = "1.2.840.113845.11.1000000001900555490.20160718102042.2434233";//Mr-fuss study
        input.setString(Tag.StudyInstanceUID, VR.UI, studyInstanceUID);
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            try (BufferedOutputStream bos = new BufferedOutputStream(byteArrayOutputStream)) {
                copyAttributesToOutput(input, bos);
                testRunner.enqueue(byteArrayOutputStream.toByteArray());
                testRunner.run(1, false, true);
                List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
                List<MockFlowFile> original = testRunner.getFlowFilesForRelationship(REL_ORIGINAL);
                List<MockFlowFile> failed = testRunner.getFlowFilesForRelationship(REL_FAILURE);
                log.info("Size of Success: {}", success.size());
                log.info("Size of original: {}", original.size());
                log.info("Size of failed: {}", failed.size());
                Assertions.assertEquals(success.size(), 300);
                Assertions.assertEquals(original.size(), 1);
                Assertions.assertEquals(failed.size(), 0);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    @Test
    public void testProcessorSuccessSeriesLevel() {

        if (!DICOM_INTEGRATION_TESTS) {
            log.info("Skipping test because DICOM INTEGRATION_TESTS");
            return;
        }

        log.info("$ $ $ $ Run GetSCU $ $ $ $ $");
        testRunner.setValidateExpressionUsage(false);
        //Patient/study level
        testRunner.setProperty(GET_LEVEL, SERIES_LEVEL);
        testRunner.setProperty(REMOTE_HOST, DICOM_SERVER_HOST);
        testRunner.setProperty(PORT, Integer.toString(DICOM_SERVER_PORT));
        final Attributes input = new Attributes();
        String studyInstanceUID = "1.2.840.113845.11.1000000001900555490.20160718102042.2434233";
        String seriesInstanceUID = "1.3.12.2.1107.5.2.19.45819.2016071811120879334462944.0.0.0";
        input.setString(Tag.StudyInstanceUID, VR.UI, studyInstanceUID);
        input.setString(Tag.SeriesInstanceUID, VR.UI, seriesInstanceUID);
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            try (BufferedOutputStream bos = new BufferedOutputStream(byteArrayOutputStream)) {
                copyAttributesToOutput(input, bos);
                testRunner.enqueue(byteArrayOutputStream.toByteArray());
                testRunner.run(1, false, true);
                List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
                List<MockFlowFile> original = testRunner.getFlowFilesForRelationship(REL_ORIGINAL);
                List<MockFlowFile> failed = testRunner.getFlowFilesForRelationship(REL_FAILURE);
                log.info("Size of Success: {}", success.size());
                log.info("Size of original: {}", original.size());
                log.info("Size of failed: {}", failed.size());
                Assertions.assertEquals(success.size(), 35);
                Assertions.assertEquals(original.size(), 1);
                Assertions.assertEquals(failed.size(), 0);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    @Test
    public void testProcessorError() {

        log.info("$ $ $ $ Run GetSCU $ $ $ $ $");
        testRunner.setValidateExpressionUsage(false);
        //Patient/study level
        testRunner.setProperty(GET_LEVEL, SERIES_LEVEL);
        testRunner.setProperty(REMOTE_HOST, DICOM_SERVER_HOST);
        testRunner.setProperty(PORT, "11114");//Integer.toString(DICOM_SERVER_PORT));
        final Attributes input = new Attributes();
        String studyInstanceUID = "1.2.840.113845.11.1000000001900555490.20160718102042.2434233";
        String seriesInstanceUID = "1.3.12.2.1107.5.2.19.45819.2016071811120879334462944.0.0.0";
        input.setString(Tag.StudyInstanceUID, VR.UI, studyInstanceUID);
        input.setString(Tag.SeriesInstanceUID, VR.UI, seriesInstanceUID);
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            try (BufferedOutputStream bos = new BufferedOutputStream(byteArrayOutputStream)) {
                copyAttributesToOutput(input, bos);
                testRunner.enqueue(byteArrayOutputStream.toByteArray());
                testRunner.run(1, false, true);
                List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
                List<MockFlowFile> original = testRunner.getFlowFilesForRelationship(REL_ORIGINAL);
                List<MockFlowFile> failed = testRunner.getFlowFilesForRelationship(REL_FAILURE);
                log.info("Size of Success: {}", success.size());
                log.info("Size of original: {}", original.size());
                log.info("Size of failed: {}", failed.size());
                Assertions.assertEquals(success.size(), 0);
                Assertions.assertEquals(original.size(), 0);
                Assertions.assertEquals(failed.size(), 1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
