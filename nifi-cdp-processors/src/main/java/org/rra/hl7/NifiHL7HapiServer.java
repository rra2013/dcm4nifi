package org.rra.hl7;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.app.Connection;
import ca.uhn.hl7v2.app.ConnectionListener;
import ca.uhn.hl7v2.app.HL7Service;
import ca.uhn.hl7v2.model.GenericMessage;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v25.message.ADT_A01;
import ca.uhn.hl7v2.parser.GenericModelClassFactory;
import ca.uhn.hl7v2.protocol.ReceivingApplication;
import ca.uhn.hl7v2.protocol.ReceivingApplicationException;
import ca.uhn.hl7v2.protocol.ReceivingApplicationExceptionHandler;
import ca.uhn.hl7v2.util.Terser;
import ca.uhn.hl7v2.util.idgenerator.InMemoryIDGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.*;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class NifiHL7HapiServer implements IHL7Server, ReceivingApplication<Message> , ConnectionListener, ReceivingApplicationExceptionHandler {

    public final static String MSG_TYPE = "MessageType";
    public final static String SEND_APP = "SendingApplication";
    public final static String SEND_FACILITY = "SendingFacility";
    public final static String RECEIVE_APP = " ReceivingApplication";
    public final static String RECEIVE_FACILITY = "ReceivingFacility";

    private final AtomicReference<ProcessSessionFactory> sessionFactory;
    private final CountDownLatch sessionFactorySetSignal;
    private final Relationship relationshipSuccess;
    private final static HapiContext context = new DefaultHapiContext();
    private HL7Service server;

    static {
        context.getParserConfiguration().setIdGenerator(new InMemoryIDGenerator());
        context.getParserConfiguration().setValidating(false);
    }

    public NifiHL7HapiServer(AtomicReference<ProcessSessionFactory> sessionFactory, CountDownLatch sessionFactorySetSignal, Relationship relSuccess) {
        this.sessionFactory = sessionFactory;
        this.sessionFactorySetSignal = sessionFactorySetSignal;
        this.relationshipSuccess = relSuccess;
    }
    private ProcessSession createProcessSession() throws InterruptedException, TimeoutException {
        ProcessSessionFactory processSessionFactory = getProcessSessionFactory();
        return processSessionFactory.createSession();
    }

    private ProcessSessionFactory getProcessSessionFactory() throws InterruptedException, TimeoutException {
        if (sessionFactorySetSignal.await(10000, TimeUnit.MILLISECONDS)) {
            return sessionFactory.get();
        } else {
            throw new TimeoutException("Waiting period for sessionFactory is over.");
        }
    }

    @Override
    public void startServer(int port, String msgType, String trigger) {
        boolean useTls = false; // Should we use TLS/SSL?
        context.setModelClassFactory(new GenericModelClassFactory());
        server = context.newServer(port, useTls);
        server.registerApplication(msgType, trigger, this);
        server.registerConnectionListener(this);
        server.setExceptionHandler(this);
        try {
            server.startAndWait();
        } catch (InterruptedException e) {
            throw new ProcessException("HL7 server could not be started.", e);
        }
        log.info("HL7 Server started. OK.");
    }

    @Override
    public void stopServer() {
        server.stopAndWait();
        log.info("HL7 Server stopped. OK.");
    }

    @Override
    public Message processMessage(Message message, Map<String, Object> map) throws ReceivingApplicationException, HL7Exception {
        String encodedMessage = context.getPipeParser().encode(message);
        log.debug("Received message:\n" + encodedMessage.replaceAll("\\r", "\r\n") + "\n\n");
        try {
            createFlowFile(encodedMessage);
        } catch (Exception e) {
           log.error("Error: ", e);
        } finally {
            // Now generate a simple acknowledgment message and return it
            try {
                return message.generateACK();
            } catch (IOException e) {
                throw new HL7Exception(e);
            }
        }
    }

    private void createFlowFile(final String encodedMessage) throws Exception {
        try {
            final ProcessSession processSession = createProcessSession();
            FlowFile flowFile = processSession.create();
            try {
                long t1 = System.nanoTime();
                String sendingApp;
                String sendingFacility;
                String receivingApp;
                String receivingFacility;
                String msgCode;
                String trigEvent;
                try (OutputStream flowFileOutputStream = processSession.write(flowFile)) {
                    copyMessage(encodedMessage, flowFileOutputStream);
                    GenericMessage msg =  (GenericMessage) context.getPipeParser().parse(encodedMessage);
                    Terser t = new Terser(msg);
                    sendingApp = t.get("/MSH-3-1");
                    sendingFacility = t.get("/MSH-4-1");
                    receivingApp = t.get("/MSH-5-1");
                    receivingFacility = t.get("/MSH-6-1");
                    msgCode = t.get("/MSH-9-1");
                    trigEvent = t.get("/MSH-9-2");
                } catch (SocketException socketException) {
                    log.error("Socket exception during data transfer", socketException);
                    processSession.rollback();
                    throw new IOException(socketException.getMessage());
                } catch (IOException ioException) {
                    log.error("IOException during data transfer", ioException);
                    processSession.rollback();
                    throw new IOException(ioException.getMessage());
                }
                try {
                    flowFile = processSession.putAttribute(flowFile, SEND_APP , sendingApp);
                    flowFile = processSession.putAttribute(flowFile, SEND_FACILITY, sendingFacility);
                    flowFile = processSession.putAttribute(flowFile, RECEIVE_APP, receivingApp);
                    flowFile = processSession.putAttribute(flowFile, RECEIVE_FACILITY , receivingFacility);
                    flowFile = processSession.putAttribute(flowFile, MSG_TYPE , msgCode+"^"+trigEvent);
                    String fileName = flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".hl7";
                    flowFile = processSession.putAttribute(flowFile, CoreAttributes.FILENAME.key(), fileName);
                    //Transfer text/plain
                    flowFile = processSession.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "text/plain");

                    final long importNanos = System.nanoTime() - t1;
                    final long importMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);
                    processSession.getProvenanceReporter().receive(flowFile, "HL7/MLLP", importMillis);

                    processSession.transfer(flowFile, relationshipSuccess);
                } catch (Exception exception) {
                    processSession.rollback();
                    log.error("Process session error. ", exception);
                }
                processSession.commitAsync(() -> {
                    // if data transfer ok - send transfer complete message
                    log.info("# # # Process Complete # # #");
                });
            } catch (Exception e) {
                throw e;
            }
        } catch (Exception exception) {
            log.error("ProcessSession could not be acquired, receive aborted.", exception);
            throw exception;
        }
    }

    private static void copyMessage(String encodedMessage, OutputStream flowFileOutputStream) throws IOException {
        try (BufferedOutputStream bos = new BufferedOutputStream(flowFileOutputStream)) {
            try (ByteArrayInputStream ba = new ByteArrayInputStream(encodedMessage.getBytes(StandardCharsets.UTF_8))) {
                try (BufferedInputStream input = new BufferedInputStream(ba)) {
                    IOUtils.copy(input, bos);
                }
            }
        }
    }

    @Override
    public boolean canProcess(Message message) {
        return true;
    }

    @Override
    public void connectionReceived(Connection connection) {
        log.info("New connection received: {}" , connection.getRemoteAddress().toString());
        log.info("From Remote Port: " + connection.getRemotePort());
    }

    @Override
    public void connectionDiscarded(Connection connection) {
        log.info("Lost connection from: {}" , connection.getRemoteAddress().toString());
        log.info("For Remote Port: " + connection.getRemotePort());
    }

    @Override
    public String processException(String theIncomingMessage, Map<String, Object> theIncomingMetadata, String theOutgoingMessage, Exception theE) {

        /*
         * Here you can do any processing you like. If you want to change
         * the response (NAK) message which will be returned you may do
         * so, or just return the NAK which HAPI already created (theOutgoingMessage)
         */

        return theOutgoingMessage;
    }
}
