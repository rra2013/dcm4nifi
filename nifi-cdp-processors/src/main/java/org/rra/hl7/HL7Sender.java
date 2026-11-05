package org.rra.hl7;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.app.Connection;
import ca.uhn.hl7v2.app.Initiator;
import ca.uhn.hl7v2.llp.LLPException;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.util.idgenerator.InMemoryIDGenerator;

import java.io.IOException;

public class HL7Sender {
    private final static boolean useTls = false;
    private final static HapiContext context = new DefaultHapiContext();
    static {
        context.getParserConfiguration().setIdGenerator(new InMemoryIDGenerator());
        context.getParserConfiguration().setValidating(false);
    }
    public static String send(String host, int port, String msg) throws HL7Exception, LLPException, IOException {
        Message message = context.getPipeParser().parse(msg);
        Connection connection = context.newClient(host, port, useTls);
        Initiator initiator = connection.getInitiator();
        Message response = initiator.sendAndReceive(message);
        connection.close();
        return context.getPipeParser().encode(response);
    };
}
