package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rra.cmove.NifiMoveSCU;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.rra.dcm.DicomUtils.*;
import static org.rra.processors.DataForTest.*;
import static org.rra.processors.MoveScu.*;

@Slf4j
public class MoveScuTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(MoveScu.class);
    }

    @Test
    public void testProcessorSuccess() {

        if (!DICOM_INTEGRATION_TESTS){
            log.info("Skipping test because DICOM INTEGRATION_TESTS");
            return;
        }

        log.info("$ $ $ $ Run MoveScu $ $ $ $ $");
        testRunner.setValidateExpressionUsage(false);
        //Patient/study level
        testRunner.setProperty(MOVE_LEVEL, STUDY_LEVEL);
        testRunner.setProperty(REMOTE_HOST, DICOM_SERVER_HOST);
        testRunner.setProperty(PORT, Integer.toString(DICOM_SERVER_PORT));
        final Attributes input = new Attributes();
        String studyInstanceUID = "1.2.840.113845.11.1000000001900555490.20160718102042.2434233";
        input.setString(Tag.StudyInstanceUID, VR.UI, studyInstanceUID);
        try(ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()){
            try(BufferedOutputStream bos = new BufferedOutputStream(byteArrayOutputStream)){
                copyAttributesToOutput(input, bos);
                testRunner.enqueue(byteArrayOutputStream.toByteArray());
                testRunner.run();
                List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
                List<MockFlowFile> failed = testRunner.getFlowFilesForRelationship(REL_FAILURE);
                log.info("Size of Success: {}", success.size());
                log.info("Size of failed: {}", failed.size());
                testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //@Test
    public void testSCUMoveSeries() throws Exception {
        NifiMoveSCU nifiMoveSCU = new NifiMoveSCU(DICOM_SERVER_HOST, DICOM_SERVER_PORT, "MOVE_SCU", DICOM_SERVER_AET, DICOM_SERVER_MOVE_AET);
        String studyInstanceUID = "1.2.840.113845.11.1000000001900555490.20160718102042.2434233";
        String seriesInstanceUID = "1.3.12.2.1107.5.2.19.45819.2016071811120879334462944.0.0.0";
        nifiMoveSCU.moveSeries(studyInstanceUID, seriesInstanceUID);
    }
    //@Test
    public void testSCUMoveStudy() throws Exception {
        NifiMoveSCU nifiMoveSCU = new NifiMoveSCU(DICOM_SERVER_HOST, DICOM_SERVER_PORT, "MOVE_SCU", DICOM_SERVER_AET, DICOM_SERVER_MOVE_AET);
        String studyInstanceUID = "1.2.840.113845.11.1000000001900555490.20160718102042.2434233";
        nifiMoveSCU.moveStudy(studyInstanceUID);
    }
}