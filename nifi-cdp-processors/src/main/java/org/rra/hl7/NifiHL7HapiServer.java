package org.rra.hl7;

import ca.uhn.hl7v2.AcknowledgmentCode;
import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.app.Connection;
import ca.uhn.hl7v2.app.ConnectionListener;
import ca.uhn.hl7v2.app.HL7Service;
import ca.uhn.hl7v2.llp.MinLowerLayerProtocol;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.GenericModelClassFactory;
import ca.uhn.hl7v2.protocol.ReceivingApplication;
import ca.uhn.hl7v2.protocol.ReceivingApplicationException;
import ca.uhn.hl7v2.protocol.ReceivingApplicationExceptionHandler;
import ca.uhn.hl7v2.util.Terser;
import ca.uhn.hl7v2.util.idgenerator.InMemoryIDGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HL7 MLLP server based on HAPI that writes every received message as a FlowFile.
 * <p>
 * A positive ACK (AA) is only returned once the FlowFile has been committed to the
 * NiFi repositories. On any failure an AE is returned so that the sender can retry
 * instead of assuming the message was delivered.
 */
@Slf4j
public class NifiHL7HapiServer implements IHL7Server, ReceivingApplication<Message>, ConnectionListener, ReceivingApplicationExceptionHandler {

    public final static String MSG_TYPE = "MessageType";
    public final static String SEND_APP = "SendingApplication";
    public final static String SEND_FACILITY = "SendingFacility";
    public final static String RECEIVE_APP = "ReceivingApplication";
    public final static String RECEIVE_FACILITY = "ReceivingFacility";
    public final static String VERSION = "Version";

    /** Max. wait for the ProcessSessionFactory (set by the first onTrigger). */
    static final long DEFAULT_SESSION_FACTORY_TIMEOUT_MS = 10_000;
    /** Max. wait for the session commit before the sender gets an AE. */
    static final long DEFAULT_COMMIT_TIMEOUT_MS = 10_000;

    private final long sessionFactoryTimeoutMs;
    private final long commitTimeoutMs;

    private final AtomicReference<ProcessSessionFactory> sessionFactory;
    private final CountDownLatch sessionFactorySetSignal;
    private final Relationship relationshipSuccess;
    /** One context per server instance: not shared between processors / restarts. */
    private final HapiContext context;
    private HL7Service server;

    public NifiHL7HapiServer(AtomicReference<ProcessSessionFactory> sessionFactory, CountDownLatch sessionFactorySetSignal, Relationship relSuccess) {
        this(sessionFactory, sessionFactorySetSignal, relSuccess, DEFAULT_SESSION_FACTORY_TIMEOUT_MS, DEFAULT_COMMIT_TIMEOUT_MS);
    }

    /** Visible for tests: allows short timeouts. */
    NifiHL7HapiServer(AtomicReference<ProcessSessionFactory> sessionFactory, CountDownLatch sessionFactorySetSignal, Relationship relSuccess,
                      long sessionFactoryTimeoutMs, long commitTimeoutMs) {
        this.sessionFactoryTimeoutMs = sessionFactoryTimeoutMs;
        this.commitTimeoutMs = commitTimeoutMs;
        this.sessionFactory = sessionFactory;
        this.sessionFactorySetSignal = sessionFactorySetSignal;
        this.relationshipSuccess = relSuccess;
        this.context = new DefaultHapiContext();
        this.context.getParserConfiguration().setIdGenerator(new InMemoryIDGenerator());
        this.context.getParserConfiguration().setValidating(false);
        this.context.setModelClassFactory(new GenericModelClassFactory());
    }

    private ProcessSession createProcessSession() throws InterruptedException, TimeoutException {
        if (!sessionFactorySetSignal.await(sessionFactoryTimeoutMs, TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Waiting period for sessionFactory is over.");
        }
        ProcessSessionFactory factory = sessionFactory.get();
        if (factory == null) {
            throw new IllegalStateException("ProcessSessionFactory not available (processor stopping?)");
        }
        return factory.createSession();
    }

    @Override
    public void startServer(int port, String msgType, String trigger, Charset charset) {
        boolean useTls = false;
        // HAPI defaults to US-ASCII (umlauts become '?'). Use the configured charset,
        // but let MSH-18 of the incoming message take precedence.
        MinLowerLayerProtocol llp = new MinLowerLayerProtocol(true);
        llp.setCharset(charset);
        context.setLowerLayerProtocol(llp);
        server = context.newServer(port, useTls);
        server.registerApplication(msgType, trigger, this);
        server.registerConnectionListener(this);
        server.setExceptionHandler(this);
        try {
            server.startAndWait();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProcessException("HL7 server could not be started.", e);
        }
        // startAndWait() does NOT throw if the socket could not be bound (e.g. port still
        // in use after a quick stop/start): HAPI only stores the exception and isRunning()
        // even stays true -> check the stored exception explicitly.
        Throwable cause = server.getServiceExitedWithException();
        if (cause != null || !server.isRunning()) {
            throw new ProcessException("HL7 server is not listening on port " + port
                    + (cause != null ? ": " + cause.getMessage() : ""), cause);
        }
        log.info("HL7 Server started on port {} (charset {}). OK.", port, charset);
    }

    @Override
    public void stopServer() {
        try {
            if (server != null) {
                server.stopAndWait();
            }
        } finally {
            try {
                context.close();
            } catch (IOException e) {
                log.warn("Could not close HAPI context", e);
            }
            server = null;
        }
        log.info("HL7 Server stopped. OK.");
    }

    @Override
    public Message processMessage(Message message, Map<String, Object> map) throws ReceivingApplicationException, HL7Exception {
        String encodedMessage = context.getPipeParser().encode(message);
        if (log.isDebugEnabled()) {
            log.debug("Received message:\n{}\n", encodedMessage.replaceAll("\\r", "\r\n"));
        }
        try {
            createFlowFile(message, encodedMessage);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("Message could not be written to NiFi, sending AE to sender.", e);
            try {
                return message.generateACK(AcknowledgmentCode.AE,
                        new HL7Exception("Message could not be stored: " + e.getMessage()));
            } catch (IOException io) {
                throw new HL7Exception(io);
            }
        }
        try {
            return message.generateACK();
        } catch (IOException e) {
            throw new HL7Exception(e);
        }
    }

    /**
     * Creates, transfers and commits a FlowFile for the message. Throws on any error;
     * the session is rolled back in that case and nothing is transferred.
     */
    private void createFlowFile(final Message message, final String encodedMessage) throws Exception {
        final long t1 = System.nanoTime();
        final ProcessSession processSession = createProcessSession();
        try {
            final Map<String, String> attributes = extractAttributes(message);

            FlowFile flowFile = processSession.create();
            final byte[] content = encodedMessage.getBytes(StandardCharsets.UTF_8);
            flowFile = processSession.write(flowFile, out -> out.write(content));

            attributes.put(CoreAttributes.FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".hl7");
            attributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");
            flowFile = processSession.putAllAttributes(flowFile, attributes);

            final long importMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1);
            processSession.getProvenanceReporter().receive(flowFile, "HL7/MLLP", importMillis);
            processSession.transfer(flowFile, relationshipSuccess);
        } catch (Exception e) {
            processSession.rollback();
            throw e;
        }

        // Only ACK after the data is safely in the repositories.
        final CompletableFuture<Void> committed = new CompletableFuture<>();
        processSession.commitAsync(() -> committed.complete(null), committed::completeExceptionally);
        try {
            committed.get(commitTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new ProcessException("Commit of FlowFile failed", e.getCause());
        }
        log.debug("FlowFile committed ({} ms)", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1));
    }

    /** Null-safe extraction of the MSH header fields (empty fields become ""). */
    private static Map<String, String> extractAttributes(Message message) throws HL7Exception {
        Terser t = new Terser(message);
        Map<String, String> attributes = new HashMap<>();
        attributes.put(VERSION, nvl(message.getVersion()));
        attributes.put(SEND_APP, nvl(t.get("/MSH-3-1")));
        attributes.put(SEND_FACILITY, nvl(t.get("/MSH-4-1")));
        attributes.put(RECEIVE_APP, nvl(t.get("/MSH-5-1")));
        attributes.put(RECEIVE_FACILITY, nvl(t.get("/MSH-6-1")));
        attributes.put(MSG_TYPE, nvl(t.get("/MSH-9-1")) + "^" + nvl(t.get("/MSH-9-2")));
        return attributes;
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }

    @Override
    public boolean canProcess(Message message) {
        return true;
    }

    @Override
    public void connectionReceived(Connection connection) {
        log.info("New connection received: {}:{}", connection.getRemoteAddress(), connection.getRemotePort());
    }

    @Override
    public void connectionDiscarded(Connection connection) {
        log.info("Lost connection from: {}:{}", connection.getRemoteAddress(), connection.getRemotePort());
    }

    @Override
    public String processException(String theIncomingMessage, Map<String, Object> theIncomingMetadata, String theOutgoingMessage, Exception theE) {
        log.warn("HL7 message rejected (NAK returned): {}", theE != null ? theE.getMessage() : "unknown", theE);
        return theOutgoingMessage;
    }
}
