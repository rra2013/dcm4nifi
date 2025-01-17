package org.rra.processors;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.app.Connection;
import ca.uhn.hl7v2.app.HL7Service;
import ca.uhn.hl7v2.app.Initiator;
import ca.uhn.hl7v2.llp.LLPException;
import ca.uhn.hl7v2.model.Message;


import ca.uhn.hl7v2.model.Varies;
import ca.uhn.hl7v2.model.v22.segment.EVN;
import ca.uhn.hl7v2.model.v22.segment.MSH;
import ca.uhn.hl7v2.model.v25.datatype.CE;
import ca.uhn.hl7v2.model.v25.datatype.ST;
import ca.uhn.hl7v2.model.v25.datatype.TX;
import ca.uhn.hl7v2.model.v25.group.ORU_R01_OBSERVATION;
import ca.uhn.hl7v2.model.v25.group.ORU_R01_ORDER_OBSERVATION;
import ca.uhn.hl7v2.model.v25.message.ORU_R01;
import ca.uhn.hl7v2.model.v25.segment.OBR;
import ca.uhn.hl7v2.model.v25.segment.OBX;
import ca.uhn.hl7v2.parser.Parser;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.protocol.ReceivingApplication;
import ca.uhn.hl7v2.protocol.ReceivingApplicationException;
import ca.uhn.hl7v2.util.idgenerator.InMemoryIDGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.rra.hl7.HL7Sender;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

public class HL7Test {
    private final static HapiContext context = new DefaultHapiContext();
    private final static PipeParser parser = context.getPipeParser();

    static {
        context.getParserConfiguration().setIdGenerator(new InMemoryIDGenerator());
        context.getParserConfiguration().setValidating(false);
    }
    @Test
    public void testProcessor() throws JsonProcessingException {
        String json = "{\"id\": 1,\"name\": \"Lokesh\"}";

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(json);
        System.out.println("jsonNode = " + jsonNode);
        XmlMapper xmlMapper = new XmlMapper();
        String xml = xmlMapper.writeValueAsString(jsonNode);
        System.out.println(xml);
        Assertions.assertEquals("<ObjectNode><id>1</id><name>Lokesh</name></ObjectNode>", xml);
    }
    @Test
    public void testHL7() throws IOException, HL7Exception {
        String msg = "MSH|^~\\&|HIS|RIH|EKG|EKG|199904140038||ADT^A01|12345|P|2.2\r"
                + "PID|0001|00009874|00001122|A00977|SMITH^JOHN^M|MOM|19581119|F|NOTREAL^LINDA^M|C|564 SPRING ST^^NEEDHAM^MA^02494^US|0002|(818)565-1551|(425)828-3344|E|S|C|0000444444|252-00-4414||||SA|||SA||||NONE|V1|0001|I|D.ER^50A^M110^01|ER|P00055|11B^M011^02|070615^BATMAN^GEORGE^L|555888^NOTREAL^BOB^K^DR^MD|777889^NOTREAL^SAM^T^DR^MD^PHD|ER|D.WT^1A^M010^01|||ER|AMB|02|070615^NOTREAL^BILL^L|ER|000001916994|D||||||||||||||||GDD|WA|NORM|02|O|02|E.IN^02D^M090^01|E.IN^01D^M080^01|199904072124|199904101200|199904101200||||5555112333|||666097^NOTREAL^MANNY^P\r"
                + "NK1|0222555|NOTREAL^JAMES^R|FA|STREET^OTHER STREET^CITY^ST^55566|(222)111-3333|(888)999-0000|||||||ORGANIZATION\r"
                + "PV1|0001|I|D.ER^1F^M950^01|ER|P000998|11B^M011^02|070615^BATMAN^GEORGE^L|555888^OKNEL^BOB^K^DR^MD|777889^NOTREAL^SAM^T^DR^MD^PHD|ER|D.WT^1A^M010^01|||ER|AMB|02|070615^VOICE^BILL^L|ER|000001916994|D||||||||||||||||GDD|WA|NORM|02|O|02|E.IN^02D^M090^01|E.IN^01D^M080^01|199904072124|199904101200|||||5555112333|||666097^DNOTREAL^MANNY^P\r"
                + "PV2|||0112^TESTING|55555^PATIENT IS NORMAL|NONE|||19990225|19990226|1|1|TESTING|555888^NOTREAL^BOB^K^DR^MD||||||||||PROD^003^099|02|ER||NONE|19990225|19990223|19990316|NONE\r"
                + "AL1||SEV|001^POLLEN\r"
                + "GT1||0222PL|NOTREAL^BOB^B||STREET^OTHER STREET^CITY^ST^77787|(444)999-3333|(222)777-5555||||MO|111-33-5555||||NOTREAL GILL N|STREET^OTHER STREET^CITY^ST^99999|(111)222-3333\r"
                + "IN1||022254P|4558PD|BLUE CROSS|STREET^OTHER STREET^CITY^ST^00990||(333)333-6666||221K|LENIX|||19980515|19990515|||PATIENT01 TEST D||||||||||||||||||02LL|022LP554";
        try (ByteArrayInputStream bas = new ByteArrayInputStream(msg.getBytes())) {
            try (BufferedInputStream bis = new BufferedInputStream(bas)) {
                String result = IOUtils.toString(bis, StandardCharsets.UTF_8);
                Message hapiMsg = parser.parse(result);
                String encode = hapiMsg.encode().replaceAll("\\r", "\r\n");
                System.out.println(encode);
                String version = hapiMsg.getVersion();
                Message ack = hapiMsg.generateACK();
                System.out.println("version = " + version);
                System.out.println("ack = " + ack);

                if (version.equals("2.2")) {
                    ca.uhn.hl7v2.model.v22.message.ADT_A01 adtA01 =
                            (ca.uhn.hl7v2.model.v22.message.ADT_A01) hapiMsg;
                    MSH msh = adtA01.getMSH();
                    EVN evn = adtA01.getEVN();
                    evn.getDateTimeOfEvent().getTimeOfAnEvent().setValue("20250116000000");

                    System.out.println("adtA01 = " + adtA01.encode().replaceAll("\\r", "\r\n"));

                }
                //String encode = hapiMsg.encode().replaceAll("\\r", "\r\n");
                /*try(ByteArrayInputStream bais = new ByteArrayInputStream(encode.getBytes())) {
                    IOUtils.copy(bais, System.out);
                }*/
            }
        }
    }
    @Test
    public void testHL7_modify() throws HL7Exception, IOException {
        ORU_R01 message = new ORU_R01();
        /*
         * The initQuickstart method populates all of the mandatory fields in the
         * MSH segment of the message, including the message type, the timestamp,
         * and the control ID.
         */
        message.initQuickstart("ORU", "R01", "T");
        /*
         * The OBR segment is contained within a group called ORDER_OBSERVATION,
         * which is itself in a group called PATIENT_RESULT. These groups are
         * reached using named accessors.
         */
        ORU_R01_ORDER_OBSERVATION orderObservation = message.getPATIENT_RESULT().getORDER_OBSERVATION();

        // Populate the OBR
        OBR obr = orderObservation.getOBR();
        obr.getSetIDOBR().setValue("1");
        obr.getFillerOrderNumber().getEntityIdentifier().setValue("1234");
        obr.getFillerOrderNumber().getNamespaceID().setValue("LAB");
        obr.getUniversalServiceIdentifier().getIdentifier().setValue("88304");

        /*
         * The OBX segment is in a repeating group called OBSERVATION. You can
         * use a named accessor which takes an index to access a specific
         * repetition. You can ask for an index which is equal to the
         * current number of repetitions,and a new repetition will be created.
         */
        ORU_R01_OBSERVATION observation = orderObservation.getOBSERVATION(0);

        // Populate the first OBX
        OBX obx = observation.getOBX();
        obx.getSetIDOBX().setValue("1");
        obx.getObservationIdentifier().getIdentifier().setValue("88304");
        obx.getObservationSubID().setValue("1");

        // The first OBX has a value type of CE. So first, we populate OBX-2 with "CE"...
        obx.getValueType().setValue("CE");

        // ... then we create a CE instance to put in OBX-5.
        CE ce = new CE(message);
        ce.getIdentifier().setValue("T57000");
        ce.getText().setValue("GALLBLADDER");
        ce.getNameOfCodingSystem().setValue("SNM");
        Varies value = obx.getObservationValue(0);
        value.setData(ce);

        // Now we populate the second OBX
        obx = orderObservation.getOBSERVATION(1).getOBX();
        obx.getSetIDOBX().setValue("2");
        obx.getObservationSubID().setValue("1");

        // The second OBX in the sample message has an extra subcomponent at
        // OBX-3-1. This component is actually an ST, but the HL7 specification allows
        // extra subcomponents to be tacked on to the end of a component. This is
        // uncommon, but HAPI nontheless allows it.
        ST observationIdentifier = obx.getObservationIdentifier().getIdentifier();
        observationIdentifier.setValue("88304");
        ST extraSubcomponent = new ST(message);
        extraSubcomponent.setValue("MDT");
        observationIdentifier.getExtraComponents().getComponent(0).setData(extraSubcomponent );

        // The first OBX has a value type of TX. So first, we populate OBX-2 with "TX"...
        obx.getValueType().setValue("TX");

        // ... then we create a CE instance to put in OBX-5.
        TX tx = new TX(message);
        tx.setValue("MICROSCOPIC EXAM SHOWS HISTOLOGICALLY NORMAL GALLBLADDER TISSUE");
        value = obx.getObservationValue(0);
        value.setData(tx);

        // Print the message (remember, the MSH segment was not fully or correctly populated)
        System.out.println(message.encode().replaceAll("\\r", "\r\n"));

    }
    @Test
    public void testHL7_send() throws HL7Exception, IOException, InterruptedException, LLPException {
        int port = 12011; // The port to listen on
        boolean useTls = false; // Should we use TLS/SSL?
        HL7Service server = context.newServer(port, useTls);
        String msg = "MSH|^~\\&|HIS|RIH|EKG|EKG|199904140038||ADT^A01|12345|P|2.2\r"
                    + "PID|0001|00009874|00001122|A00977|SMITH^JOHN^M|MOM|19581119|F|NOTREAL^LINDA^M|C|564 SPRING ST^^NEEDHAM^MA^02494^US|0002|(818)565-1551|(425)828-3344|E|S|C|0000444444|252-00-4414||||SA|||SA||||NONE|V1|0001|I|D.ER^50A^M110^01|ER|P00055|11B^M011^02|070615^BATMAN^GEORGE^L|555888^NOTREAL^BOB^K^DR^MD|777889^NOTREAL^SAM^T^DR^MD^PHD|ER|D.WT^1A^M010^01|||ER|AMB|02|070615^NOTREAL^BILL^L|ER|000001916994|D||||||||||||||||GDD|WA|NORM|02|O|02|E.IN^02D^M090^01|E.IN^01D^M080^01|199904072124|199904101200|199904101200||||5555112333|||666097^NOTREAL^MANNY^P\r"
                     + "NK1|0222555|NOTREAL^JAMES^R|FA|STREET^OTHER STREET^CITY^ST^55566|(222)111-3333|(888)999-0000|||||||ORGANIZATION\r"
                     + "PV1|0001|I|D.ER^1F^M950^01|ER|P000998|11B^M011^02|070615^BATMAN^GEORGE^L|555888^OKNEL^BOB^K^DR^MD|777889^NOTREAL^SAM^T^DR^MD^PHD|ER|D.WT^1A^M010^01|||ER|AMB|02|070615^VOICE^BILL^L|ER|000001916994|D||||||||||||||||GDD|WA|NORM|02|O|02|E.IN^02D^M090^01|E.IN^01D^M080^01|199904072124|199904101200|||||5555112333|||666097^DNOTREAL^MANNY^P\r"
                     + "PV2|||0112^TESTING|55555^PATIENT IS NORMAL|NONE|||19990225|19990226|1|1|TESTING|555888^NOTREAL^BOB^K^DR^MD||||||||||PROD^003^099|02|ER||NONE|19990225|19990223|19990316|NONE\r"
                     + "AL1||SEV|001^POLLEN\r"
                     + "GT1||0222PL|NOTREAL^BOB^B||STREET^OTHER STREET^CITY^ST^77787|(444)999-3333|(222)777-5555||||MO|111-33-5555||||NOTREAL GILL N|STREET^OTHER STREET^CITY^ST^99999|(111)222-3333\r"
                     + "IN1||022254P|4558PD|BLUE CROSS|STREET^OTHER STREET^CITY^ST^00990||(333)333-6666||221K|LENIX|||19980515|19990515|||PATIENT01 TEST D||||||||||||||||||02LL|022LP554";

        ReceivingApplication<Message> handler = new ExampleReceiverApplication();
        server.registerApplication("ADT", "A01", handler);
        server.startAndWait();

        //Message adt = parser.parse(msg);
        /*Connection connection = context.newClient("localhost", port, useTls);
        // The initiator is used to transmit unsolicited messages
        Initiator initiator = connection.getInitiator();
        Message response = initiator.sendAndReceive(adt);
        String responseString = parser.encode(response);*/
        for (int i=0; i< 100; ++i){
            String responseString = HL7Sender.send("localhost", port, msg);
            System.out.println(i+"Received response:\n" + responseString.replaceAll("\\r", "\r\n"));
        }
        //connection.close();
        server.stopAndWait();
    }

    public class ExampleReceiverApplication implements ReceivingApplication<Message>{
        @Override
        public Message processMessage(Message message, Map<String, Object> map) throws ReceivingApplicationException, HL7Exception {
            try {
                return message.generateACK();
            } catch (IOException e) {
                throw new HL7Exception(e);
            }
        }

        @Override
        public boolean canProcess(Message message) {
            return true;
        }
    }
}
