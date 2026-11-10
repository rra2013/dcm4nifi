package org.rra.processors;

import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.rra.hl7.IHL7Server;
import org.rra.hl7.NifiHL7HapiServer;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;


@Tags({"HL7", "server", "MLLP", "CDP"})
@CapabilityDescription("A HL7 MLLP server based on HAPI. A positive ACK (AA) is only returned after the message has been committed "
        + "as a FlowFile; otherwise the sender receives an AE and can retry.")
@UseCase(description = "Receives HL7 Objects via TCP/IP and MLLP. Listening on port and bind to IP address. ")
@WritesAttributes({
        @WritesAttribute(attribute = NifiHL7HapiServer.VERSION, description = "HL7 version (MSH-12)"),
        @WritesAttribute(attribute = NifiHL7HapiServer.SEND_APP, description = "Sending application (MSH-3-1)"),
        @WritesAttribute(attribute = NifiHL7HapiServer.SEND_FACILITY, description = "Sending facility (MSH-4-1)"),
        @WritesAttribute(attribute = NifiHL7HapiServer.RECEIVE_APP, description = "Receiving application (MSH-5-1)"),
        @WritesAttribute(attribute = NifiHL7HapiServer.RECEIVE_FACILITY, description = "Receiving facility (MSH-6-1)"),
        @WritesAttribute(attribute = NifiHL7HapiServer.MSG_TYPE, description = "Message code and trigger event, e.g. ORM^O01"),
        @WritesAttribute(attribute = "filename", description = "<uuid>.hl7"),
        @WritesAttribute(attribute = "mime.type", description = "text/plain")})
public class HL7Server extends AbstractSessionFactoryProcessor {

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("listening-port")
            .displayName("Listening Port")
            .description("The Port to listen on for incoming connections. On Linux, root privileges are required to use port numbers below 1024.")
            .required(true)
            .defaultValue("5000")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor MESSAGE_TYPE = new PropertyDescriptor.Builder()
            .name("message-type")
            .displayName("Message Type")
            .description("The message type of the HL7 than only be accepted. For example ADT. With * all types will be accepted")
            .required(true)
            .defaultValue("*")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    public static final PropertyDescriptor TRIGGER_EVENT = new PropertyDescriptor.Builder()
            .name("trigger-event")
            .displayName("Trigger Event")
            .description("The trigger event of the message that only be accepted. For example A01. With * all events would be accepted.")
            .required(false)
            .defaultValue("*")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("character-set")
            .displayName("Character Set")
            .description("Character set of the incoming MLLP stream. If a message contains MSH-18, that character set takes precedence. "
                    + "The FlowFile content is always written as UTF-8.")
            .required(true)
            .defaultValue("UTF-8")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Receive HL7 success")
            .build();
    private final AtomicReference<ProcessSessionFactory> sessionFactory = new AtomicReference<>();
    private volatile CountDownLatch sessionFactorySetSignal;
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private IHL7Server server;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(PORT, MESSAGE_TYPE, TRIGGER_EVENT, CHARACTER_SET);
        relationships = Set.of(REL_SUCCESS);
    }

    @OnScheduled
    public void startHL7Server(final ProcessContext context) {
        final ComponentLog log = getLogger();
        if (null == server) {
            sessionFactory.set(null);
            int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
            String msgType = context.getProperty(MESSAGE_TYPE).evaluateAttributeExpressions().getValue();
            String trigger = context.getProperty(TRIGGER_EVENT).evaluateAttributeExpressions().getValue();
            Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).evaluateAttributeExpressions().getValue());
            try {
                sessionFactorySetSignal = new CountDownLatch(1);
                server = new NifiHL7HapiServer(sessionFactory, sessionFactorySetSignal, REL_SUCCESS);
                server.startServer(port, msgType, trigger, charset);
            } catch (RuntimeException startException) {
                log.error("HL7 server could not be started: {}", startException.getMessage(), startException);
                stopHL7Server();
                throw startException;
            }
        } else {
            log.info("Server is all ready started");
        }
    }

    @OnStopped
    public void stopHL7Server() {
        final ComponentLog log = getLogger();
        log.info("+ + + Stop the HL7 Server + + + ");
        try {
            if (null != server) server.stopServer();
        } catch (RuntimeException e) {
            log.warn("Error while stopping HL7 server", e);
        } finally {
            server = null;
            sessionFactory.set(null);
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        if (this.sessionFactory.compareAndSet(null, sessionFactory)) {
            final CountDownLatch signal = sessionFactorySetSignal;
            if (signal != null) signal.countDown();
        }
        context.yield();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

}
