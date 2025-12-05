package org.rra.processors;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.groovy.json.internal.Dates;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcm4che3.data.*;
import org.dcm4che3.data.Tag;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;
import org.rra.dcm.DicomUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class PseudonymizerTest {
    final static String DB_LOCATION = "target/db";

    private static List<byte[]> dcmObjects = new ArrayList<>();
    private static List<byte[]> rtObjects = new ArrayList<>();
    private TestRunner runner;

    @BeforeAll
    public static void readData() {
        //Get DICOM Files
        dcmObjects = DataForTest.DCMOBJECTS;
        rtObjects = DataForTest.DCM_RT_OBJECTS;
        Assertions.assertTrue(dcmObjects.size() > 0);
        Assertions.assertTrue(rtObjects.size() > 0);
    }

    @BeforeEach
    public void init() throws InitializationException {
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();
        runner = TestRunners.newTestRunner(Pseudonymizer.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(Pseudonymizer.DBCP_SERVICE, "dbcp");
    }

    @Test
    public void testIncomingConnectionWithDSPos() throws SQLException {
        testWithDateShift(5);
    }

    @Test
    public void testIncomingConnectionWithDSNeg() throws SQLException {
        testWithDateShift(-500);
    }
    @Test
    public void testIncomingConnectionWithDSNull() throws SQLException {
        testWithDateShift(0);
    }

    @Test
    public void testIncomingConnectionWithNoDS() throws SQLException {
        testWithOutDateShift();
    }

    private void testWithDateShift(int dateShift) throws SQLException {
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_PSEUDONYMIZER");
        } catch (final SQLException ignored) {
        }

        Calendar cal = Calendar.getInstance();
        List<ShiftDates> dates = new ArrayList<>();

        stmt.execute("create table TEST_PSEUDONYMIZER (id integer not null, pid varchar(45), prefix varchar(50),postfix varchar(45), date_shift integer not null ,constraint my_pk primary key (id))");
        stmt.execute("insert into TEST_PSEUDONYMIZER (id, pid, prefix, postfix, date_shift) VALUES (0,'4025765337', 'PRE89898BK', '164' , "+dateShift+")");
        runner.setIncomingConnection(true);
        runner.setProperty(Pseudonymizer.SQL_SELECT_QUERY, "SELECT pid, prefix, postfix, date_shift FROM TEST_PSEUDONYMIZER where pid=?");
        //prepare Input
        dcmObjects.forEach(dcmFileArray -> {
            Attributes dcm = DicomUtils.byteArrayToAttributes(dcmFileArray);
            ShiftDates shiftDates = new ShiftDates();
            shiftDates.setAcqDate(dcm.getDate(Tag.AcquisitionDate));
            shiftDates.setAcqDateTime(dcm.getDate(Tag.AcquisitionDateTime));
            shiftDates.setStudyDate(dcm.getDate(Tag.StudyDate));
            shiftDates.setSeriesDate(dcm.getDate(Tag.SeriesDate));
            shiftDates.setContentDate(dcm.getDate(Tag.ContentDate));
            dates.add(shiftDates);

            HashMap<String, String> attr = new HashMap<>();
            attr.put("CallingAET", "TEST_RUNNER");
            runner.enqueue(dcmFileArray, attr);
            runner.run();
        });
        //Assert all are done in success
        runner.assertAllFlowFilesTransferred(Pseudonymizer.REL_SUCCESS);
        // Read out put
        List<MockFlowFile> success = runner.getFlowFilesForRelationship(Pseudonymizer.REL_SUCCESS);
        AtomicInteger ctr = new AtomicInteger(0);
        success.forEach(mockFlowFile -> {
            byte[] readAnonym = mockFlowFile.toByteArray();
            Attributes dcm = DicomUtils.byteArrayToAttributes(readAnonym);
            String pid = dcm.getString(Tag.PatientID);
            String name = dcm.getString(Tag.PatientName);
            String issuer = dcm.getString(Tag.IssuerOfPatientID);
            Date acqDate = dcm.getDate(Tag.AcquisitionDate);
            Date acqDateTime = dcm.getDate(Tag.AcquisitionDateTime);
            Date studyDate = dcm.getDate(Tag.StudyDate);
            Date seriesDate = dcm.getDate(Tag.SeriesDate);
            Date contentDate = dcm.getDate(Tag.ContentDate);
            log.info(" + + + PatientID: {}, Patientname:{}, IssuerOfpatID:{}", pid, name, issuer);
            Assertions.assertEquals("PRE89898BK-164", pid);
            Assertions.assertEquals("PRE89898BK^164", name);
            Assertions.assertEquals("IDSC_DCMA", issuer);
            //Check date shift
            if (dateShift == 0){
                //If no date shift than the tag will be removed by deidentifier and must be null in the output
                Assertions.assertEquals(null, acqDate);
                Assertions.assertEquals(null, acqDateTime);
                Assertions.assertEquals(null, studyDate);
                Assertions.assertEquals(null, seriesDate);
                String input = "19991111";
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                try {
                    Date date = sdf.parse(input);
                    Assertions.assertEquals(date, contentDate);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }else{
                ShiftDates sd = dates.get(ctr.getAndIncrement());
                Date dateOrig = sd.getAcqDate();
                if (null != dateOrig){ //Check acq date
                    cal.setTime(dateOrig);
                    cal.add(Calendar.DAY_OF_MONTH, dateShift);
                    log.info("Date orig: {}, new: {}", dateOrig, acqDate);
                    Assertions.assertEquals(acqDate, cal.getTime());
                }
                dateOrig = sd.getAcqDateTime();
                if (null != dateOrig){ //Check acq date tile
                    cal.setTime(dateOrig);
                    cal.add(Calendar.DAY_OF_MONTH, dateShift);
                    log.info("Date orig: {}, new: {}", dateOrig, acqDateTime);
                    Assertions.assertEquals(acqDateTime, cal.getTime());
                }
                dateOrig = sd.getStudyDate();
                if (null != dateOrig){ //Check acq date tile
                    cal.setTime(dateOrig);
                    cal.add(Calendar.DAY_OF_MONTH, dateShift);
                    log.info("Date orig: {}, new: {}", dateOrig, studyDate);
                    Assertions.assertEquals(studyDate, cal.getTime());
                }
                dateOrig = sd.getSeriesDate();
                if (null != dateOrig){ //Check acq date tile
                    cal.setTime(dateOrig);
                    cal.add(Calendar.DAY_OF_MONTH, dateShift);
                    log.info("Date orig: {}, new: {}", dateOrig, seriesDate);
                    Assertions.assertEquals(seriesDate, cal.getTime());
                }
                dateOrig = sd.getContentDate();
                if (null != dateOrig){ //Check acq date tile
                    cal.setTime(dateOrig);
                    cal.add(Calendar.DAY_OF_MONTH, dateShift);
                    log.info("Date orig: {}, new: {}", dateOrig, contentDate);
                    Assertions.assertEquals(contentDate, cal.getTime());
                }
            }
            //
            String studyIUID_dcm = dcm.getString(Tag.StudyInstanceUID);
            String seriesIUID_dcm = dcm.getString(Tag.SeriesInstanceUID);
            String studyIUID = mockFlowFile.getAttribute("StudyInstanceUID");
            Assertions.assertTrue(studyIUID.equals(studyIUID_dcm));
            String seriesIUD = mockFlowFile.getAttribute("SeriesInstanceUID");
            Assertions.assertTrue(seriesIUD.equals(seriesIUID_dcm));
        });
    }

    @Test
    public void retainTagsProcessTest() throws Exception {

        //Test with date shift and retain AcquisitionDate and AccessionNumber
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_PSEUDONYMIZER");
        } catch (final SQLException ignored) {
        }

        List<Date> acqDates = new ArrayList<>();
        List<String> accNr = new ArrayList<>();
        List<String> sopIUIDs = new ArrayList<>();


        stmt.execute("create table TEST_PSEUDONYMIZER (id integer not null, pid varchar(45), prefix varchar(50),postfix varchar(45), date_shift integer not null ,constraint my_pk primary key (id))");
        stmt.execute("insert into TEST_PSEUDONYMIZER (id, pid, prefix, postfix, date_shift) VALUES (0,'4025765337', 'PRE89898BK', '164' , 0)");
        runner.setIncomingConnection(true);
        runner.setProperty(Pseudonymizer.SQL_SELECT_QUERY, "SELECT pid, prefix, postfix FROM TEST_PSEUDONYMIZER where pid=?");
        runner.setProperty(Pseudonymizer.RETAIN_TAGS, "AcquisitionDate, AccessionNumber, SOPInstanceUID");
        //prepare Input
        dcmObjects.forEach(dcmFileArray -> {
            Attributes dcm = DicomUtils.byteArrayToAttributes(dcmFileArray);
            // save attribute
            acqDates.add(dcm.getDate(Tag.AcquisitionDate));
            accNr.add(dcm.getString(Tag.AccessionNumber, ""));
            sopIUIDs.add(dcm.getString(Tag.SOPInstanceUID, ""));
            //
            HashMap<String, String> attr = new HashMap<>();
            attr.put("CallingAET", "TEST_RUNNER");
            runner.enqueue(dcmFileArray, attr);
            runner.run();
        });
        //Assert all are done in success
        runner.assertAllFlowFilesTransferred(Pseudonymizer.REL_SUCCESS);
        // Read out put
        List<MockFlowFile> success = runner.getFlowFilesForRelationship(Pseudonymizer.REL_SUCCESS);
        AtomicInteger ctrAcc = new AtomicInteger(0);
        success.forEach(mockFlowFile -> {
            byte[] readAnonym = mockFlowFile.toByteArray();
            Attributes dcm = DicomUtils.byteArrayToAttributes(readAnonym);
            String pid = dcm.getString(Tag.PatientID);
            String name = dcm.getString(Tag.PatientName);
            String issuer = dcm.getString(Tag.IssuerOfPatientID);

            Date acqDate = dcm.getDate(Tag.AcquisitionDate);
            String accessionNumber = dcm.getString(Tag.AccessionNumber);
            String sopIUID = dcm.getString(Tag.SOPInstanceUID);

            Assertions.assertEquals("PRE89898BK-164", pid);
            Assertions.assertEquals("PRE89898BK^164", name);
            Assertions.assertEquals("IDSC_DCMA", issuer);
            log.info(" + + + PatientID: {}, Patientname:{}, IssuerOfpatID:{}", pid, name, issuer);
            // Assert retain tags
            String accNrVerify = accNr.get(ctrAcc.get());
            Date acqDateVerify = acqDates.get(ctrAcc.get());
            String sopIUIDVerify = sopIUIDs.get(ctrAcc.get());
            ctrAcc.incrementAndGet();
            //
            Assertions.assertEquals(accessionNumber, accNrVerify);
            Assertions.assertEquals(acqDate, acqDateVerify);
            Assertions.assertEquals(sopIUID, sopIUIDVerify);
            //
            String studyIUID_dcm = dcm.getString(Tag.StudyInstanceUID);
            String seriesIUID_dcm = dcm.getString(Tag.SeriesInstanceUID);
            String studyIUID = mockFlowFile.getAttribute("StudyInstanceUID");
            Assertions.assertTrue(studyIUID.equals(studyIUID_dcm));
            String seriesIUD = mockFlowFile.getAttribute("SeriesInstanceUID");
            Assertions.assertTrue(seriesIUD.equals(seriesIUID_dcm));
        });

    }

    @Test
    public void retainTagsProcessTestRT() throws Exception {

        //
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_PSEUDONYMIZER");
        } catch (final SQLException ignored) {
        }

        Map<String,String> retains = new HashMap<>();


        stmt.execute("create table TEST_PSEUDONYMIZER (id integer not null, pid varchar(45), prefix varchar(50),postfix varchar(45), date_shift integer not null ,constraint my_pk primary key (id))");
        stmt.execute("insert into TEST_PSEUDONYMIZER (id, pid, prefix, postfix, date_shift) VALUES (0,'4025765337', 'PRE89898BK', '164' , 0)");
        stmt.execute("insert into TEST_PSEUDONYMIZER (id, pid, prefix, postfix, date_shift) VALUES (1,'0008722285', 'PRE89898BK', '165' , 0)");
        stmt.execute("insert into TEST_PSEUDONYMIZER (id, pid, prefix, postfix, date_shift) VALUES (2,'0001900919', 'PRE89898BK', '166' , 0)");
        runner.setIncomingConnection(true);
        runner.setProperty(Pseudonymizer.SQL_SELECT_QUERY, "SELECT pid, prefix, postfix FROM TEST_PSEUDONYMIZER where pid=?");
        runner.setProperty(Pseudonymizer.RETAIN_TAGS, "SOPInstanceUID,FrameOfReferenceUID");
        //prepare Input
        rtObjects.forEach(dcmFileArray -> {
            Attributes dcm = DicomUtils.byteArrayToAttributes(dcmFileArray);
            // save attribute
            String frame = dcm.getString(Tag.FrameOfReferenceUID,null);
            String sop = dcm.getString(Tag.SOPInstanceUID,null);
            if (null != sop ) {
                retains.put(sop, frame);
            }
            log.info("SOP:Frame : {},{}" ,sop, frame);
            //
            HashMap<String, String> attr = new HashMap<>();
            attr.put("CallingAET", "TEST_RUNNER");
            runner.enqueue(dcmFileArray, attr);
            runner.run();
        });
        //Assert all are done in success
        runner.assertAllFlowFilesTransferred(Pseudonymizer.REL_SUCCESS);
        // Read out put
        List<MockFlowFile> success = runner.getFlowFilesForRelationship(Pseudonymizer.REL_SUCCESS);
        AtomicInteger ctrAcc = new AtomicInteger(0);
        success.forEach(mockFlowFile -> {
            byte[] readAnonym = mockFlowFile.toByteArray();
            Attributes dcm = DicomUtils.byteArrayToAttributes(readAnonym);
            String frame = dcm.getString(Tag.FrameOfReferenceUID);
            String sop = dcm.getString(Tag.SOPInstanceUID);
            log.info(">>>>> SOP:Frame : {},{}" ,sop, frame);
            String frameVerify = retains.get(sop);
            Assertions.assertEquals(frame, frameVerify);
        });

    }

    @Test
    public void retainTagsInSeqProcessTest() throws Exception {

        //Test with date shift and retain AcquisitionDate and AccessionNumber
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_PSEUDONYMIZER");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_PSEUDONYMIZER (id integer not null, pid varchar(45), prefix varchar(50),postfix varchar(45), date_shift integer not null ,constraint my_pk primary key (id))");
        stmt.execute("insert into TEST_PSEUDONYMIZER (id, pid, prefix, postfix, date_shift) VALUES (0,'0001900919', 'PRE89898BK', '164' , 0)");
        stmt.execute("insert into TEST_PSEUDONYMIZER (id, pid, prefix, postfix, date_shift) VALUES (1,'0008722285', 'PRE89898BK', '165' , 0)");
        runner.setIncomingConnection(true);
        runner.setProperty(Pseudonymizer.SQL_SELECT_QUERY, "SELECT pid, prefix, postfix FROM TEST_PSEUDONYMIZER where pid=?");
        runner.setProperty(Pseudonymizer.RETAIN_TAGS, "ReferencedSOPInstanceUID, SOPInstanceUID");
        HashMap<String, Sequence> retainMap = new HashMap<>();
        rtObjects.forEach(rtFileArray -> {
            Attributes dcm = DicomUtils.byteArrayToAttributes(rtFileArray);
            //log.info("Patient ID {}",dcm.getString(Tag.PatientID));
            //

            Sequence sequence = dcm.getSequence(Tag.ReferencedStructureSetSequence);
            if (sequence != null) {
                sequence.forEach(sequenceItem -> {
                    String refSop = sequenceItem.getString(Tag.ReferencedSOPInstanceUID, null);
                    if (null != refSop) {
                        log.info(" - " + refSop);
                    }
                });
                String sopIUID = dcm.getString(Tag.SOPInstanceUID);
                retainMap.put(sopIUID, sequence);
            }
            HashMap<String, String> attr = new HashMap<>();
            attr.put("CallingAET", "TEST_RUNNER");
            runner.enqueue(rtFileArray, attr);
            runner.run();
        });
        //Assert all are done in success
        runner.assertAllFlowFilesTransferred(Pseudonymizer.REL_SUCCESS);
        // Read out put
        List<MockFlowFile> success = runner.getFlowFilesForRelationship(Pseudonymizer.REL_SUCCESS);
        success.forEach(mockFlowFile -> {
            byte[] readAnonym = mockFlowFile.toByteArray();
            Attributes dcm = DicomUtils.byteArrayToAttributes(readAnonym);
            // Assert retain tags
            Sequence sequence = dcm.getSequence(Tag.ReferencedStructureSetSequence);
            if (sequence != null) {
                sequence.forEach(sequenceItem -> {
                    String refSop = sequenceItem.getString(Tag.ReferencedSOPInstanceUID, null);
                    if (null != refSop) {
                        log.info(" + " + refSop);
                        String sopIUID = dcm.getString(Tag.SOPInstanceUID);
                        Sequence attributes = retainMap.get(sopIUID);
                        attributes.forEach(attributeItem -> {
                            String refSopVerify = attributeItem.getString(Tag.ReferencedSOPInstanceUID, null);
                            if (null != refSopVerify) {
                                log.info(" - " + refSopVerify);
                                Assertions.assertEquals(refSop, refSopVerify);
                            }
                        });
                    }
                });
            }
            //

        });
        //
        // Test errors
        //
        runner.setProperty(Pseudonymizer.RETAIN_TAGS, "SOPInstanceUID");
        runner.clearTransferState();

        rtObjects.forEach(rtFileArray -> {
            Attributes dcm = DicomUtils.byteArrayToAttributes(rtFileArray);
            //
            Sequence sequence = dcm.getSequence(Tag.ReferencedStructureSetSequence);
            if (sequence != null) {
                sequence.forEach(sequenceItem -> {
                    String refSop = sequenceItem.getString(Tag.ReferencedSOPInstanceUID, null);
                    if (null != refSop) {
                        log.info(" % " + refSop);
                    }
                });
            }
            HashMap<String, String> attr = new HashMap<>();
            attr.put("CallingAET", "TEST_RUNNER");
            runner.enqueue(rtFileArray, attr);
            runner.run();
        });
        runner.assertAllFlowFilesTransferred(Pseudonymizer.REL_SUCCESS);

        success = runner.getFlowFilesForRelationship(Pseudonymizer.REL_SUCCESS);
        success.forEach(mockFlowFile -> {
            byte[] readAnonym = mockFlowFile.toByteArray();
            Attributes dcm = DicomUtils.byteArrayToAttributes(readAnonym);
            // Assert retain tags
            Sequence sequence = dcm.getSequence(Tag.ReferencedStructureSetSequence);
            if (sequence != null) {
                sequence.forEach(sequenceItem -> {
                    String refSop = sequenceItem.getString(Tag.ReferencedSOPInstanceUID, null);
                    if (null != refSop) {
                        log.info(" + " + refSop);
                        String sopIUID = dcm.getString(Tag.SOPInstanceUID);
                        Sequence attributes = retainMap.get(sopIUID);
                        attributes.forEach(attributeItem -> {
                            String refSopVerify = attributeItem.getString(Tag.ReferencedSOPInstanceUID, null);
                            if (null != refSopVerify) {
                                log.info(" - " + refSopVerify);
                                Assertions.assertNotEquals(refSop, refSopVerify);
                            }
                        });
                    }
                });
            }
            //

        });

    }


    @Test
    public void retainSequenceProcessTest() throws Exception {

        //Test with date shift and retain AcquisitionDate and AccessionNumber
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_PSEUDONYMIZER");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_PSEUDONYMIZER (id integer not null, pid varchar(45), prefix varchar(50),postfix varchar(45), date_shift integer not null ,constraint my_pk primary key (id))");
        stmt.execute("insert into TEST_PSEUDONYMIZER (id, pid, prefix, postfix, date_shift) VALUES (0,'0001900919', 'PRE89898BK', '164' , 0)");
        stmt.execute("insert into TEST_PSEUDONYMIZER (id, pid, prefix, postfix, date_shift) VALUES (1,'0008722285', 'PRE89898BK', '165' , 0)");
        runner.setIncomingConnection(true);
        runner.setProperty(Pseudonymizer.SQL_SELECT_QUERY, "SELECT pid, prefix, postfix FROM TEST_PSEUDONYMIZER where pid=?");
        runner.setProperty(Pseudonymizer.RETAIN_TAGS, "FrameOfReferenceUID, ReferencedSOPInstanceUID, SOPInstanceUID, RTReferencedStudySequence, RTReferencedSeriesSequence");
        HashMap<String, Sequence> retainMapRTRefStudy = new HashMap<>();
        rtObjects.forEach(rtFileArray -> {
            Attributes dcm = DicomUtils.byteArrayToAttributes(rtFileArray);
            Sequence refFrame = dcm.getSequence(Tag.ReferencedFrameOfReferenceSequence);
            if (refFrame != null) {
                refFrame.forEach(referenceItem -> {
                    log.info("FrameOfReferenceUID {}",referenceItem.getString(Tag.FrameOfReferenceUID));
                    Sequence sequence = referenceItem.getSequence(Tag.RTReferencedStudySequence);
                    if (sequence != null) {
                        String sopIUID = dcm.getString(Tag.SOPInstanceUID);
                        retainMapRTRefStudy.put(sopIUID, sequence);
                    }
                });
            }


            HashMap<String, String> attr = new HashMap<>();
            attr.put("CallingAET", "TEST_RUNNER");
            runner.enqueue(rtFileArray, attr);
            runner.run();
        });
        //Assert all are done in success
        runner.assertAllFlowFilesTransferred(Pseudonymizer.REL_SUCCESS);
        // Read out put
        List<MockFlowFile> success = runner.getFlowFilesForRelationship(Pseudonymizer.REL_SUCCESS);
        success.forEach(mockFlowFile -> {
            byte[] readAnonym = mockFlowFile.toByteArray();
            Attributes anonym = DicomUtils.byteArrayToAttributes(readAnonym);
            String sopIUID = anonym.getString(Tag.SOPInstanceUID);
            // Assert retain tags
            Sequence original = retainMapRTRefStudy.get(sopIUID);
            if (null != original) {
                Sequence refFrame = anonym.getSequence(Tag.ReferencedFrameOfReferenceSequence);
                Assertions.assertNotNull(refFrame);

                refFrame.forEach(referenceItem -> {
                    log.info("FrameOfReferenceUID {}",referenceItem.getString(Tag.FrameOfReferenceUID));
                    Sequence sequence = referenceItem.getSequence(Tag.RTReferencedStudySequence);
                    Assertions.assertNotNull(sequence);
                    Assertions.assertEquals(original.size(), sequence.size());
                });

            }


            //
        });


    }

    @Test
    public void retainTagsProcessTestWithDateshift() throws Exception {
        /*
            Date shift overrides retain tags
         */
        //Test with date shift and retain AcquisitionDate and AccessionNumber
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_PSEUDONYMIZER");
        } catch (final SQLException ignored) {
        }

        List<Date> acqDates = new ArrayList<>();
        List<Date> studyDates = new ArrayList<>();
        List<String> accNr = new ArrayList<>();

        stmt.execute("create table TEST_PSEUDONYMIZER (id integer not null, pid varchar(45), prefix varchar(50),postfix varchar(45), date_shift integer not null ,constraint my_pk primary key (id))");
        stmt.execute("insert into TEST_PSEUDONYMIZER (id, pid, prefix, postfix, date_shift) VALUES (0,'4025765337', 'PRE89898BK', '164' , 10)");
        runner.setIncomingConnection(true);
        runner.setProperty(Pseudonymizer.SQL_SELECT_QUERY, "SELECT pid, prefix, postfix, date_shift FROM TEST_PSEUDONYMIZER where pid=?");
        runner.setProperty(Pseudonymizer.RETAIN_TAGS, "AcquisitionDate, AccessionNumber, StudyDate");
        //prepare Input
        dcmObjects.forEach(dcmFileArray -> {
            Attributes dcm = DicomUtils.byteArrayToAttributes(dcmFileArray);
            // save attribute
            acqDates.add(dcm.getDate(Tag.AcquisitionDate));
            studyDates.add(dcm.getDate(Tag.StudyDate));
            accNr.add(dcm.getString(Tag.AccessionNumber, ""));
            //
            HashMap<String, String> attr = new HashMap<>();
            attr.put("CallingAET", "TEST_RUNNER");
            runner.enqueue(dcmFileArray, attr);
            runner.run();
        });
        //Assert all are done in success
        runner.assertAllFlowFilesTransferred(Pseudonymizer.REL_SUCCESS);
        // Read out put
        List<MockFlowFile> success = runner.getFlowFilesForRelationship(Pseudonymizer.REL_SUCCESS);
        AtomicInteger ctrAcc = new AtomicInteger(0);
        success.forEach(mockFlowFile -> {
            byte[] readAnonym = mockFlowFile.toByteArray();
            Attributes dcm = DicomUtils.byteArrayToAttributes(readAnonym);
            String pid = dcm.getString(Tag.PatientID);
            String name = dcm.getString(Tag.PatientName);
            String issuer = dcm.getString(Tag.IssuerOfPatientID);

            Date acqDate = dcm.getDate(Tag.AcquisitionDate);
            Date studyDate = dcm.getDate(Tag.StudyDate);
            String accessionNumber = dcm.getString(Tag.AccessionNumber);

            Assertions.assertEquals("PRE89898BK-164", pid);
            Assertions.assertEquals("PRE89898BK^164", name);
            Assertions.assertEquals("IDSC_DCMA", issuer);
            log.info(" + + + PatientID: {}, Patientname:{}, IssuerOfpatID:{}", pid, name, issuer);
            // Assert retain tags
            String accNrVerify = accNr.get(ctrAcc.get());
            Date acqDateVerify = acqDates.get(ctrAcc.get());
            Date studyDateVerify = studyDates.get(ctrAcc.get());
            ctrAcc.incrementAndGet();
            //
            Assertions.assertEquals(accessionNumber, accNrVerify);
            //
            // Date shift overrides retain tags
            Assertions.assertNotEquals(acqDate, acqDateVerify); // Date is shifted
            Assertions.assertNotEquals(studyDate, studyDateVerify); // date is shifted
            log.info(" + + + ds:StudyDate: {}, orig:StudyDate:{}", studyDate, studyDateVerify);
            //
            String studyIUID_dcm = dcm.getString(Tag.StudyInstanceUID);
            String seriesIUID_dcm = dcm.getString(Tag.SeriesInstanceUID);
            String studyIUID = mockFlowFile.getAttribute("StudyInstanceUID");
            Assertions.assertTrue(studyIUID.equals(studyIUID_dcm));
            String seriesIUD = mockFlowFile.getAttribute("SeriesInstanceUID");
            Assertions.assertTrue(seriesIUD.equals(seriesIUID_dcm));
        });

    }

    @Test
    public void retainTagsEvalTest() {
        final String sTag = "AcquisitionDate";
        final int iTag = Tag.AcquisitionDate;
        VR vr = ElementDictionary.getStandardElementDictionary().vrOf(iTag);
        int tag = ElementDictionary.getStandardElementDictionary().tagForKeyword(sTag);
        String name = ElementDictionary.getStandardElementDictionary().keywordOf(tag);
        Assertions.assertEquals(iTag, tag);
        Assertions.assertTrue(vr == VR.DA);
        Assertions.assertTrue(name.equals("AcquisitionDate"));
    }

    private void testWithOutDateShift() throws SQLException {
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_PSEUDONYMIZER");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_PSEUDONYMIZER (id integer not null, pid varchar(45), prefix varchar(50),postfix varchar(45), constraint my_pk primary key (id))");
        stmt.execute("insert into TEST_PSEUDONYMIZER (id, pid, prefix, postfix) VALUES (0,'4025765337', 'PRE89898BK', '164')");
        runner.setIncomingConnection(true);
        runner.setProperty(Pseudonymizer.SQL_SELECT_QUERY, "SELECT pid, prefix, postfix FROM TEST_PSEUDONYMIZER where pid=?");
        //prepare Input
        dcmObjects.forEach(dcmFileArray -> {
            HashMap<String, String> attr = new HashMap<>();
            attr.put("CallingAET", "TEST_RUNNER");
            runner.enqueue(dcmFileArray, attr);
            runner.run();
        });
        //Assert all are done in success
        runner.assertAllFlowFilesTransferred(Pseudonymizer.REL_SUCCESS);
        // Read out put
        List<MockFlowFile> success = runner.getFlowFilesForRelationship(Pseudonymizer.REL_SUCCESS);
        AtomicInteger ctr = new AtomicInteger(0);
        success.forEach(mockFlowFile -> {
            byte[] readAnonym = mockFlowFile.toByteArray();
            Attributes dcm = DicomUtils.byteArrayToAttributes(readAnonym);
            String pid = dcm.getString(Tag.PatientID);
            String name = dcm.getString(Tag.PatientName);
            String issuer = dcm.getString(Tag.IssuerOfPatientID);
            Date acqDate = dcm.getDate(Tag.AcquisitionDate);
            log.info(" + + + PatientID: {}, Patientname:{}, IssuerOfpatID:{}", pid, name, issuer);
            Assertions.assertEquals("PRE89898BK-164", pid);
            Assertions.assertEquals("PRE89898BK^164", name);
            Assertions.assertEquals("IDSC_DCMA", issuer);
            //
            String studyIUID_dcm = dcm.getString(Tag.StudyInstanceUID);
            String seriesIUID_dcm = dcm.getString(Tag.SeriesInstanceUID);
            String studyIUID = mockFlowFile.getAttribute("StudyInstanceUID");
            Assertions.assertTrue(studyIUID.equals(studyIUID_dcm));
            String seriesIUD = mockFlowFile.getAttribute("SeriesInstanceUID");
            Assertions.assertTrue(seriesIUD.equals(seriesIUID_dcm));
        });
    }

    @Test
    public void testIncomingConnectionFail() throws SQLException {
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();
        try {
            stmt.execute("drop table TEST_PSEUDONYMIZER");
        } catch (final SQLException ignored) {
        }
        stmt.execute("create table TEST_PSEUDONYMIZER (id integer not null, pid varchar(45), prefix varchar(50),postfix varchar(45),date_shift integer not null ,constraint my_pk primary key (id))");
        stmt.execute("insert into TEST_PSEUDONYMIZER (id, pid, prefix, postfix, date_shift) VALUES (0,'wrong_pid','PRE89898BK', 'POST164', 10 )");
        runner.setIncomingConnection(true);
        runner.setProperty(Pseudonymizer.SQL_SELECT_QUERY, "SELECT pid, prefix, postfix, date_shift FROM TEST_PSEUDONYMIZER where pid=?");
        dcmObjects.forEach(dcmFileArray -> {
            HashMap<String, String> attr = new HashMap<>();
            attr.put("CallingAET", "TEST_RUNNER");
            runner.enqueue(dcmFileArray, attr);
            runner.run();
        });
        runner.assertAllFlowFilesTransferred(Pseudonymizer.REL_FAILURE);
    }

    @BeforeAll
    public static void setupClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
        //System.setProperty("derby.system.home", "/home/dev/.derby");
    }

    @AfterAll
    public static void cleanupClass() {
        System.clearProperty("derby.stream.error.file");
    }


    class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                final Connection con = DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true");
                return Mockito.spy(con);
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class ShiftDates{
        private Date acqDate;
        private Date acqDateTime;
        private Date studyDate;
        private Date seriesDate;
        private Date contentDate;
    }
}
