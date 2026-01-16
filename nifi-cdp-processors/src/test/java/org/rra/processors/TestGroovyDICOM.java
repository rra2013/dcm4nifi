package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.processors.groovyx.ExecuteGroovyScript;
import org.apache.nifi.util.*;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Sequence;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.rra.dcm.DicomUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
@DisabledOnOs(OS.WINDOWS)
public class TestGroovyDICOM {
    protected ExecuteGroovyScript proc;
    protected TestRunner runner;
    public static final String TEST_RESOURCE_LOCATION = "target/test/resources/groovy/";
    public static final String LIB_RESOURCE_LOCATION = "target/test/resources/lib/";

    private static List<byte[]> dcmObjects = new ArrayList<>();
    private static List<byte[]> rtObjects = new ArrayList<>();

    /**
     * Copies all scripts to the target directory because when they are
     * compiled they can leave unwanted .class files.
     *
     * @throws Exception Any error encountered while testing
     */
    @BeforeAll
    public static void setupBeforeClass() throws Exception {
        FileUtils.copyDirectory(new File("src/test/resources"), new File("target/test/resources"));
        FileUtils.copyDirectory(new File("../groovy/script"), new File("target/test/resources/groovy/"));
        FileUtils.copyDirectory(new File("../groovy/lib"), new File(LIB_RESOURCE_LOCATION));

        //Get DICOM Files
        dcmObjects = DataForTest.DCMOBJECTS;
        rtObjects = DataForTest.DCM_RT_OBJECTS;
        Assertions.assertTrue(dcmObjects.size() > 0);
        Assertions.assertTrue(rtObjects.size() > 0);
    }

    @Test
    public void test_dicom_compare_groovy(){
        runner.setProperty(ExecuteGroovyScript.ADD_CLASSPATH, LIB_RESOURCE_LOCATION);
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "dicom_attribute_comparator.groovy");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");
        runner.assertValid();

        dcmObjects.forEach(bytesArrayDcm -> {
            runner.enqueue(bytesArrayDcm);
            runner.run();
        });
        List<MockFlowFile> success = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS);
        runner.assertTransferCount(ExecuteGroovyScript.REL_SUCCESS, success.size());

        List<MockFlowFile> failed = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_FAILURE);
        runner.assertTransferCount(ExecuteGroovyScript.REL_FAILURE, failed.size());
        log.info("Count of success:{}, failed:{}", success.size(), failed.size());
    }

    @Test
    public void test_modify_dicom_sequence_groovy(){
        runner.setProperty(ExecuteGroovyScript.ADD_CLASSPATH, LIB_RESOURCE_LOCATION);
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "modify_RTStructureSetStorage.groovy");
        runner.assertValid();

        Map<String,String> attributes = new HashMap<>();
        attributes.put("SeriesInstanceUID", "2.25.12.23.34.4.55676.567657.555.1");

        rtObjects.forEach(bytesArrayDcm -> {
            runner.enqueue(bytesArrayDcm, attributes);
            runner.run();
        });

        List<MockFlowFile> success = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS);
        log.info("Count of success:{}", success.size());
        runner.assertTransferCount(ExecuteGroovyScript.REL_SUCCESS, success.size());
        success.forEach(mockFlowFile -> {
            byte[] readAnonym = mockFlowFile.toByteArray();
            Attributes ds = DicomUtils.byteArrayToAttributes(readAnonym);
            String sopCUID = ds.getString(Tag.SOPClassUID);
            log.info("sopCUID:{}", sopCUID);
            String pid = ds.getString(Tag.PatientID);
            log.info("pid:{}", pid);
            //
            Sequence rforSeq = ds.getSequence(Tag.ReferencedFrameOfReferenceSequence);
            if (rforSeq == null || rforSeq.isEmpty()) {
                throw new IllegalStateException("ReferencedFrameOfReferenceSequence fehlt/leer");
            }
            Attributes rforItem = rforSeq.get(0);
            Sequence rtRefStudySeq = rforItem.getSequence(Tag.RTReferencedStudySequence);
            if (rtRefStudySeq == null || rtRefStudySeq.isEmpty()) {
                throw new IllegalStateException("RTReferencedStudySequence fehlt/leer");
            }
            Attributes rtRefStudyItem = rtRefStudySeq.get(0);
            Sequence rtRefSeriesSeq = rtRefStudyItem.getSequence(Tag.RTReferencedSeriesSequence);
            if (rtRefSeriesSeq == null || rtRefSeriesSeq.isEmpty()) {
                throw new IllegalStateException("RTReferencedSeriesSequence fehlt/leer");
            }
            Attributes rtRefSeriesItem = rtRefSeriesSeq.get(0);
            // SeriesInstanceUID in der Sequence
            String seriesIUID = rtRefSeriesItem.getString(Tag.SeriesInstanceUID);
            // must be equal
            Assertions.assertEquals(seriesIUID, attributes.get("SeriesInstanceUID"));
        });

        List<MockFlowFile> error = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_FAILURE);
        log.info("Count of failed:{}", error.size());
    }


    @BeforeEach
    public void setup() {
        //init processor
        proc = new ExecuteGroovyScript();
        MockProcessContext context = new MockProcessContext(proc);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(proc, context);
        proc.initialize(initContext);
        assertNotNull(proc.getSupportedPropertyDescriptors());
        runner = TestRunners.newTestRunner(proc);
    }

}
