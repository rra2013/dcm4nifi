package org.rra.hl7;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.app.Connection;
import ca.uhn.hl7v2.app.ConnectionListener;
import ca.uhn.hl7v2.app.HL7Service;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.protocol.ReceivingApplication;
import ca.uhn.hl7v2.protocol.ReceivingApplicationException;
import ca.uhn.hl7v2.protocol.ReceivingApplicationExceptionHandler;
import ca.uhn.hl7v2.util.idgenerator.InMemoryIDGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;

import java.io.*;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class HL7HapiServer implements IHL7Server, ReceivingApplication<Message> , ConnectionListener, ReceivingApplicationExceptionHandler {


    private final AtomicReference<ProcessSessionFactory> sessionFactory;
    private final CountDownLatch sessionFactorySetSignal;
    private final Relationship relationshipSuccess;
    private final static HapiContext context = new DefaultHapiContext();
    private HL7Service server;

    static {
        context.getParserConfiguration().setIdGenerator(new InMemoryIDGenerator());
        context.getParserConfiguration().setValidating(false);
    }

    public HL7HapiServer(AtomicReference<ProcessSessionFactory> sessionFactory, CountDownLatch sessionFactorySetSignal, Relationship relSuccess) {
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
    public void startServer(int port, String msgType, String trigger) throws InterruptedException {
        boolean useTls = false; // Should we use TLS/SSL?
        server = context.newServer(port, useTls);
        server.registerApplication(msgType, trigger, this);
        server.registerConnectionListener(this);
        server.setExceptionHandler(this);
        server.startAndWait();
        log.info("HL7 Server started. OK.");
    }

    @Override
    public void stopServer() {
        server.stopAndWait();
        log.info("HL7 Server stopped. OK.");
    }

    @Override
    public Message processMessage(Message message, Map<String, Object> map) throws ReceivingApplicationException, HL7Exception {
        String encodedMessage = new DefaultHapiContext().getPipeParser().encode(message);
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
                try (OutputStream flowFileOutputStream = processSession.write(flowFile)) {
                    copyMessage(encodedMessage, flowFileOutputStream);
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
                    processSession.getProvenanceReporter().modifyContent(flowFile);
                    //Transfer text/plain
                    flowFile = processSession.putAttribute(flowFile, "mime.type", "text/plain");
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
            log.error("ProcessSession could not be acquired, command STOR aborted.", exception);
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
