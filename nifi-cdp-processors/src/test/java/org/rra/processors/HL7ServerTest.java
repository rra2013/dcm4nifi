package org.rra.processors;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.app.Connection;
import ca.uhn.hl7v2.app.Initiator;
import ca.uhn.hl7v2.llp.LLPException;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.Parser;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.parser.XMLParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rra.hl7.NifiHL7HapiServer;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
public class HL7ServerTest {
    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(HL7Server.class);
    }

    @Test
    public void testProcessor() throws HL7Exception, LLPException, IOException {
        final int port = Utils.provideRandomPort();
        testRunner.setProperty(HL7Server.PORT, Integer.toString(port));
        testRunner.run(1, false, true);
        log.info("$ $ $ $ Run $ $ $ $ $");
        String msg = "MSH|^~\\&|HIS|RIH|EKG|EKG|199904140038||ADT^A01|12345|P|2.2\r"
                + "PID|0001|00009874|00001122|A00977|SMITH^JOHN^M|MOM|19581119|F|NOTREAL^LINDA^M|C|564 SPRING ST^^NEEDHAM^MA^02494^US|0002|(818)565-1551|(425)828-3344|E|S|C|0000444444|252-00-4414||||SA|||SA||||NONE|V1|0001|I|D.ER^50A^M110^01|ER|P00055|11B^M011^02|070615^BATMAN^GEORGE^L|555888^NOTREAL^BOB^K^DR^MD|777889^NOTREAL^SAM^T^DR^MD^PHD|ER|D.WT^1A^M010^01|||ER|AMB|02|070615^NOTREAL^BILL^L|ER|000001916994|D||||||||||||||||GDD|WA|NORM|02|O|02|E.IN^02D^M090^01|E.IN^01D^M080^01|199904072124|199904101200|199904101200||||5555112333|||666097^NOTREAL^MANNY^P\r"
                + "NK1|0222555|NOTREAL^JAMES^R|FA|STREET^OTHER STREET^CITY^ST^55566|(222)111-3333|(888)999-0000|||||||ORGANIZATION\r"
                + "PV1|0001|I|D.ER^1F^M950^01|ER|P000998|11B^M011^02|070615^BATMAN^GEORGE^L|555888^OKNEL^BOB^K^DR^MD|777889^NOTREAL^SAM^T^DR^MD^PHD|ER|D.WT^1A^M010^01|||ER|AMB|02|070615^VOICE^BILL^L|ER|000001916994|D||||||||||||||||GDD|WA|NORM|02|O|02|E.IN^02D^M090^01|E.IN^01D^M080^01|199904072124|199904101200|||||5555112333|||666097^DNOTREAL^MANNY^P\r"
                + "PV2|||0112^TESTING|55555^PATIENT IS NORMAL|NONE|||19990225|19990226|1|1|TESTING|555888^NOTREAL^BOB^K^DR^MD||||||||||PROD^003^099|02|ER||NONE|19990225|19990223|19990316|NONE\r"
                + "AL1||SEV|001^POLLEN\r"
                + "GT1||0222PL|NOTREAL^BOB^B||STREET^OTHER STREET^CITY^ST^77787|(444)999-3333|(222)777-5555||||MO|111-33-5555||||NOTREAL GILL N|STREET^OTHER STREET^CITY^ST^99999|(111)222-3333\r"
                + "IN1||022254P|4558PD|BLUE CROSS|STREET^OTHER STREET^CITY^ST^00990||(333)333-6666||221K|LENIX|||19980515|19990515|||PATIENT01 TEST D||||||||||||||||||02LL|022LP554";
        final HapiContext context = new DefaultHapiContext();
        final Parser p = context.getPipeParser();
        Message adt = p.parse(msg);
        boolean useTls = false;
        Connection connection = context.newClient("localhost", port, useTls);
        // The initiator is used to transmit unsolicited messages
        Initiator initiator = connection.getInitiator();
        Message response = initiator.sendAndReceive(adt);

        String responseString = p.encode(response);
        log.info("Received response:\n" + responseString.replaceAll("\\r", "\r\n"));

        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(HL7Server.REL_SUCCESS);
        log.info("Count of success {}", success.size());
        Assertions.assertTrue(success.size() >= 1);

        success.forEach(mockFlowFile -> {
                    String sendApp = mockFlowFile.getAttribute(NifiHL7HapiServer.SEND_APP);
                    log.info("sendApp {}", sendApp);
                    String sendFacil = mockFlowFile.getAttribute(NifiHL7HapiServer.SEND_FACILITY);
                    log.info("sendFacil {}", sendFacil);
                    String recApp = mockFlowFile.getAttribute(NifiHL7HapiServer.RECEIVE_APP);
                    log.info("recApp {}", recApp);
                    String recFacil = mockFlowFile.getAttribute(NifiHL7HapiServer.RECEIVE_FACILITY);
                    log.info("recFacil {}", recFacil);
                    String msgType = mockFlowFile.getAttribute(NifiHL7HapiServer.MSG_TYPE);
                    log.info("msgType {}", msgType);
                    byte[] readAnonym = mockFlowFile.toByteArray();
                    try (ByteArrayInputStream ba = new ByteArrayInputStream(readAnonym)) {
                        try (BufferedInputStream bif = new BufferedInputStream(ba)) {
                            String result = IOUtils.toString(bif, StandardCharsets.UTF_8);
                            log.info("Result: \n {}", result.replaceAll("\\r", "\r\n"));
                            Message adt_processed = p.parse(result);
                            log.info(adt_processed.toString().replaceAll("\\r", "\r\n"));
                        } catch (HL7Exception e) {
                            e.printStackTrace();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
        );
    }

    @Test
    public void testValidation() throws Exception {
        String inValidMessage = "MSH|^~\\&|MedSeries|CAISI_1-2|PLS|3910|200903230934||ADT^A31^ADT_A05|75535037-1237815294895|P^T|2.4\r"
                + "EVN|A31|THIS-IS-NOT-DATE-VALUE\r"
                + "PID|1||29^^CAISI_1-2^PI~\"\"||Test300^Leticia^^^^^L||19770202|M||||||||||||||||||||||";

        HapiContext context = new DefaultHapiContext();
        context.getParserConfiguration().setValidating(false); // disable validation
        PipeParser parser = context.getPipeParser();
        Message hapiMsg = parser.parse(inValidMessage); // successfull parsed
        System.out.println(hapiMsg.toString().replaceAll("\\r", "\r\n"));
        XMLParser xmlParser = context.getXMLParser();
        String xmlDoc = xmlParser.encode(hapiMsg,"XML");
        Message parse = xmlParser.parse(xmlDoc);// validation still disabled
        System.out.println(parse.toString().replaceAll("\\r", "\r\n"));

    }

}
