package org.rra.processors;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.parser.XMLParser;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.rra.hl7.MllpTestClient;
import org.rra.hl7.NifiHL7HapiServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.rra.hl7.MllpTestClient.*;

/**
 * Integration tests of the HL7Server processor with the NiFi mock framework:
 * configuration, processor lifecycle (schedule / stop / restart) and what ends up in NiFi.
 * <p>
 * Failure paths of the server itself (commit errors, timeouts, broken connections, concurrency)
 * are covered in {@link org.rra.hl7.NifiHL7HapiServerTest}.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
public class HL7ServerTest {

    private TestRunner testRunner;
    private int port;
    /** Only stop in tearDown what was started: TestRunner.run() requires a valid configuration. */
    private boolean scheduled;

    @BeforeEach
    public void init() throws IOException {
        testRunner = TestRunners.newTestRunner(HL7Server.class);
        port = freePort();
        testRunner.setProperty(HL7Server.PORT, Integer.toString(port));
    }

    @AfterEach
    public void tearDown() {
        if (scheduled) {
            stopProcessor();
        }
    }

    // ================================================================== configuration

    @Test
    public void defaultConfigurationIsValid() {
        testRunner.assertValid();
        assertEquals("UTF-8", HL7Server.CHARACTER_SET.getDefaultValue());
        assertEquals("*", HL7Server.MESSAGE_TYPE.getDefaultValue());
        assertEquals("*", HL7Server.TRIGGER_EVENT.getDefaultValue());
    }

    @Test
    public void invalidConfigurationIsRejected() {
        testRunner.setProperty(HL7Server.PORT, "abc");
        testRunner.assertNotValid();
        testRunner.setProperty(HL7Server.PORT, "70000");
        testRunner.assertNotValid();
        testRunner.setProperty(HL7Server.PORT, Integer.toString(port));
        testRunner.assertValid();

        testRunner.setProperty(HL7Server.CHARACTER_SET, "NO-SUCH-CHARSET");
        testRunner.assertNotValid();
        testRunner.setProperty(HL7Server.CHARACTER_SET, "ISO-8859-1");
        testRunner.assertValid();

        testRunner.setProperty(HL7Server.MESSAGE_TYPE, " ");
        testRunner.assertNotValid();
        testRunner.setProperty(HL7Server.MESSAGE_TYPE, "*");
        testRunner.assertValid();
    }

    // ================================================================== receiving

    @Test
    public void receivedMessageBecomesFlowFileWithAttributesAndProvenance() throws Exception {
        startProcessor();

        String ack = send(adtA01("CTRL-1"), StandardCharsets.UTF_8);

        assertEquals("AA", ackCode(ack));
        assertEquals("CTRL-1", ackControlId(ack));
        testRunner.assertAllFlowFilesTransferred(HL7Server.REL_SUCCESS, 1);

        MockFlowFile ff = testRunner.getFlowFilesForRelationship(HL7Server.REL_SUCCESS).get(0);
        ff.assertAttributeEquals(NifiHL7HapiServer.VERSION, "2.5");
        ff.assertAttributeEquals(NifiHL7HapiServer.SEND_APP, "HIS");
        ff.assertAttributeEquals(NifiHL7HapiServer.SEND_FACILITY, "RIH");
        ff.assertAttributeEquals(NifiHL7HapiServer.RECEIVE_APP, "EKG");
        ff.assertAttributeEquals(NifiHL7HapiServer.RECEIVE_FACILITY, "EKG");
        ff.assertAttributeEquals(NifiHL7HapiServer.MSG_TYPE, "ADT^A01");
        ff.assertAttributeEquals("mime.type", "text/plain");
        assertTrue(ff.getAttribute("filename").endsWith(".hl7"));

        String content = ff.getContent();
        assertTrue(content.startsWith("MSH|^~\\&|HIS|RIH|EKG|EKG|"), content);
        assertTrue(content.contains("|CTRL-1|"));
        assertTrue(content.contains("Müller^Jürg"), "umlauts must survive (default charset UTF-8): " + content);

        List<ProvenanceEventRecord> events = testRunner.getProvenanceEvents();
        assertEquals(1, events.size());
        assertEquals(ProvenanceEventType.RECEIVE, events.get(0).getEventType());
        assertEquals("HL7/MLLP", events.get(0).getTransitUri());
    }

    @Test
    public void ormO01FromRisIsReceived() throws Exception {
        startProcessor();

        assertEquals("AA", ackCode(send(ormO01("ORM-1"), StandardCharsets.UTF_8)));

        testRunner.assertAllFlowFilesTransferred(HL7Server.REL_SUCCESS, 1);
        testRunner.getFlowFilesForRelationship(HL7Server.REL_SUCCESS).get(0)
                .assertAttributeEquals(NifiHL7HapiServer.MSG_TYPE, "ORM^O01");
    }

    @Test
    public void severalMessagesOnOneConnection() throws Exception {
        startProcessor();
        try (MllpTestClient client = new MllpTestClient(port, StandardCharsets.UTF_8)) {
            for (int i = 0; i < 10; i++) {
                assertEquals("AA", ackCode(client.sendAndReceive(adtA01("SEQ-" + i))));
            }
        }
        testRunner.assertAllFlowFilesTransferred(HL7Server.REL_SUCCESS, 10);
    }

    @Test
    public void characterSetPropertyIsUsedWhenMsh18IsMissing() throws Exception {
        testRunner.setProperty(HL7Server.CHARACTER_SET, "ISO-8859-1");
        startProcessor();

        assertEquals("AA", ackCode(send(adtA01("LATIN1"), StandardCharsets.ISO_8859_1)));

        MockFlowFile ff = testRunner.getFlowFilesForRelationship(HL7Server.REL_SUCCESS).get(0);
        assertTrue(ff.getContent().contains("Müller^Jürg"), ff.getContent());   // content is always UTF-8
    }

    @Test
    public void messageTypeAndTriggerFilter() throws Exception {
        testRunner.setProperty(HL7Server.MESSAGE_TYPE, "ORM");
        testRunner.setProperty(HL7Server.TRIGGER_EVENT, "O01");
        startProcessor();

        String rejected = send(adtA01("ADT-1"), StandardCharsets.UTF_8);
        assertNotNull(rejected, "a NAK is expected, not a dropped connection");
        assertNotEquals("AA", ackCode(rejected));
        assertEquals("AA", ackCode(send(ormO01("ORM-1"), StandardCharsets.UTF_8)));

        testRunner.assertAllFlowFilesTransferred(HL7Server.REL_SUCCESS, 1);
    }

    // ================================================================== lifecycle

    @Test
    public void scheduleFailsVisiblyIfPortIsInUse() throws Exception {
        try (ServerSocket blocker = new ServerSocket()) {
            blocker.setReuseAddress(true);
            blocker.bind(new InetSocketAddress(port));
            // @OnScheduled must fail instead of leaving a "running" processor that does not listen
            assertThrows(Throwable.class, () -> testRunner.run(1, false, true));
        }
        // NiFi retries @OnScheduled: once the port is free again it must work
        startProcessor();
        assertEquals("AA", ackCode(send(adtA01("AFTER-BLOCK"), StandardCharsets.UTF_8)));
    }

    @Test
    public void stopReleasesPortAndRestartWorks() throws Exception {
        startProcessor();
        assertEquals("AA", ackCode(send(adtA01("FIRST"), StandardCharsets.UTF_8)));

        stopProcessor();
        assertTrue(isPortFree(port), "port must be released when the processor is stopped");
        assertThrows(IOException.class, () -> new MllpTestClient(port, StandardCharsets.UTF_8).close());

        startProcessor();
        assertEquals("AA", ackCode(send(adtA01("SECOND"), StandardCharsets.UTF_8)));
        testRunner.assertAllFlowFilesTransferred(HL7Server.REL_SUCCESS, 2);
    }

    @Test
    public void stopWithoutStartIsHarmless() {
        assertDoesNotThrow(this::stopProcessor);
        assertDoesNotThrow(this::stopProcessor);
    }

    // ================================================================== parser behaviour relied upon

    @Test
    public void parserAcceptsInvalidFieldValuesWhenValidationIsDisabled() throws Exception {
        String inValidMessage = "MSH|^~\\&|MedSeries|CAISI_1-2|PLS|3910|200903230934||ADT^A31^ADT_A05|75535037-1237815294895|P^T|2.4\r"
                + "EVN|A31|THIS-IS-NOT-DATE-VALUE\r"
                + "PID|1||29^^CAISI_1-2^PI~\"\"||Test300^Leticia^^^^^L||19770202|M||||||||||||||||||||||";

        try (HapiContext context = new DefaultHapiContext()) {
            context.getParserConfiguration().setValidating(false);
            PipeParser parser = context.getPipeParser();
            Message hapiMsg = parser.parse(inValidMessage);
            XMLParser xmlParser = context.getXMLParser();
            Message roundTrip = xmlParser.parse(xmlParser.encode(hapiMsg, "XML"));   // validation still disabled
            assertTrue(parser.encode(roundTrip).contains("THIS-IS-NOT-DATE-VALUE"));
        }
    }

    // ================================================================== helpers

    /** Like NiFi starting the processor: @OnScheduled + one onTrigger (hands over the session factory). */
    private void startProcessor() {
        scheduled = true;
        testRunner.run(1, false, true);
    }

    /** Like NiFi stopping the processor: @OnUnscheduled + @OnStopped. */
    private void stopProcessor() {
        testRunner.run(1, true, false);
        scheduled = false;
    }

    private String send(String msg, Charset charset) throws IOException {
        try (MllpTestClient client = new MllpTestClient(port, charset)) {
            return client.sendAndReceive(msg);
        }
    }
}
