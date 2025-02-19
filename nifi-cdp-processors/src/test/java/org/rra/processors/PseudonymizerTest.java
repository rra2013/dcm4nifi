package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Sequence;
import org.dcm4che3.data.Tag;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;
import org.rra.dcm.DicomUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
@Slf4j
public class PseudonymizerTest {
    final static String DB_LOCATION = "target/db";

    private static List<byte[]> dcmObjects = new ArrayList<>();
    private TestRunner runner;

    @BeforeAll
    public static void readData() {
        //Get DICOM Files
        dcmObjects = DataForTest.DCMOBJECTS;
        Assertions.assertTrue(dcmObjects.size() > 0);
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
    public void testIncomingConnection() throws SQLException {
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_PSEUDONYMIZER");
        } catch (final SQLException ignored) {
        }
        stmt.execute("create table TEST_PSEUDONYMIZER (id integer not null, pid varchar(45), prefix varchar(50),postfix varchar(45) ,constraint my_pk primary key (id))");
        stmt.execute("insert into TEST_PSEUDONYMIZER (id, pid, prefix, postfix) VALUES (0,'4025765337', 'PRE89898BK', '164' )");
        runner.setIncomingConnection(true);
        runner.setProperty(Pseudonymizer.SQL_SELECT_QUERY, "SELECT pid, prefix, postfix FROM TEST_PSEUDONYMIZER where pid=?");
        dcmObjects.forEach(dcmFileArray -> {
            HashMap<String, String> attr = new HashMap<>();
            attr.put("CallingAET", "TEST_RUNNER");
            runner.enqueue(dcmFileArray, attr);
            runner.run();
        });
        runner.assertAllFlowFilesTransferred(Pseudonymizer.REL_SUCCESS);
        List<MockFlowFile> success = runner.getFlowFilesForRelationship(Pseudonymizer.REL_SUCCESS);
        success.forEach(mockFlowFile -> {
            byte[] readAnonym = mockFlowFile.toByteArray();
            Attributes dcm = DicomUtils.byteArrayToAttributes(readAnonym);
            String pid = dcm.getString(Tag.PatientID);
            String name = dcm.getString(Tag.PatientName);
            String issuer = dcm.getString(Tag.IssuerOfPatientID);
            log.info(" + + + PatientID: {}, Patientname:{}, IssuerOfpatID:{}", pid, name, issuer);
            Assertions.assertEquals("PRE89898BK-164", pid);
            Assertions.assertEquals("PRE89898BK^164", name);
            Assertions.assertEquals("DCM4NIFI", issuer);
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
        stmt.execute("create table TEST_PSEUDONYMIZER (id integer not null, pid varchar(45), prefix varchar(50),postfix varchar(45) ,constraint my_pk primary key (id))");
        stmt.execute("insert into TEST_PSEUDONYMIZER (id, pid, prefix, postfix) VALUES (0,'wrong_pid','PRE89898BK', 'POST164' )");
        runner.setIncomingConnection(true);
        runner.setProperty(Pseudonymizer.SQL_SELECT_QUERY, "SELECT pid, prefix, postfix FROM TEST_PSEUDONYMIZER where pid=?");
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
}
