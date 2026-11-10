package org.rra.hl7;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.rra.hl7.MllpTestClient.*;

/**
 * Tests of the HL7 MLLP server itself, independent of the NiFi test runner.
 * <p>
 * The NiFi session is simulated, so that every failure path (no session factory, write error,
 * commit error, commit hanging) can be forced deterministically. Core invariant checked
 * throughout: <b>AA is returned if and only if the message was committed</b>.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class NifiHL7HapiServerTest {

    private static final Relationship SUCCESS = new Relationship.Builder().name("success").build();
    private static final long SHORT_TIMEOUT_MS = 500;

    private final List<NifiHL7HapiServer> servers = new ArrayList<>();
    private FakeSessionFactory factory;
    private int port;

    @BeforeEach
    void setUp() throws IOException {
        factory = new FakeSessionFactory();
        port = freePort();
    }

    @AfterEach
    void tearDown() {
        for (NifiHL7HapiServer s : servers) {
            try {
                s.stopServer();
            } catch (RuntimeException ignored) {
                // already stopped
            }
        }
    }

    // ================================================================== happy path

    @Test
    void positiveAckAndFlowFileWithAllAttributes() throws Exception {
        start("*", "*", StandardCharsets.UTF_8, true);

        String ack = send(adtA01("CTRL-1"));

        assertEquals("AA", ackCode(ack));
        assertEquals("CTRL-1", ackControlId(ack));
        assertEquals(1, factory.committed.size());

        Stored ff = factory.committed.get(0);
        assertEquals("2.5", ff.attributes.get(NifiHL7HapiServer.VERSION));
        assertEquals("HIS", ff.attributes.get(NifiHL7HapiServer.SEND_APP));
        assertEquals("RIH", ff.attributes.get(NifiHL7HapiServer.SEND_FACILITY));
        assertEquals("EKG", ff.attributes.get(NifiHL7HapiServer.RECEIVE_APP));
        assertEquals("EKG", ff.attributes.get(NifiHL7HapiServer.RECEIVE_FACILITY));
        assertEquals("ADT^A01", ff.attributes.get(NifiHL7HapiServer.MSG_TYPE));
        assertTrue(ff.attributes.get("filename").endsWith(".hl7"));
        assertEquals("text/plain", ff.attributes.get("mime.type"));
        assertFalse(ff.attributes.containsKey(" ReceivingApplication"), "attribute name must not have a leading blank");

        String content = new String(ff.content, StandardCharsets.UTF_8);
        assertTrue(content.startsWith("MSH|^~\\&|HIS|RIH|EKG|EKG|"), content);
        assertTrue(content.contains("|CTRL-1|"));
        assertTrue(content.contains("Müller^Jürg"), "umlauts must survive: " + content);
        assertEquals(1, factory.provenanceReceives.get());
    }

    @Test
    void ormO01IsAccepted() throws Exception {
        start("*", "*", StandardCharsets.UTF_8, true);
        String ack = send(ormO01("ORM-1"));
        assertEquals("AA", ackCode(ack));
        assertEquals("ORM^O01", factory.committed.get(0).attributes.get(NifiHL7HapiServer.MSG_TYPE));
    }

    @Test
    void emptyMshFieldsResultInEmptyAttributesNotFailure() throws Exception {
        start("*", "*", StandardCharsets.UTF_8, true);

        String msg = message("ADT^A01", "CTRL-EMPTY", "2.5", "HIS", "", "", "", null, "Doe^John");
        String ack = send(msg);

        assertEquals("AA", ackCode(ack));
        Stored ff = factory.committed.get(0);
        assertEquals("HIS", ff.attributes.get(NifiHL7HapiServer.SEND_APP));
        assertEquals("", ff.attributes.get(NifiHL7HapiServer.SEND_FACILITY));
        assertEquals("", ff.attributes.get(NifiHL7HapiServer.RECEIVE_APP));
        assertEquals("", ff.attributes.get(NifiHL7HapiServer.RECEIVE_FACILITY));
    }

    @Test
    void allCommonHl7VersionsAreAccepted() throws Exception {
        start("*", "*", StandardCharsets.UTF_8, true);
        String[] versions = {"2.2", "2.3", "2.3.1", "2.4", "2.5", "2.5.1", "2.6", "2.7", "2.8"};
        try (MllpTestClient client = new MllpTestClient(port, StandardCharsets.UTF_8)) {
            for (String v : versions) {
                String ack = client.sendAndReceive(message("ADT^A01", "V" + v, v, "HIS", "RIH", "EKG", "EKG", null, "Doe^John"));
                assertEquals("AA", ackCode(ack), "version " + v);
            }
        }
        assertEquals(versions.length, factory.committed.size());
        for (int i = 0; i < versions.length; i++) {
            assertEquals(versions[i], factory.committed.get(i).attributes.get(NifiHL7HapiServer.VERSION));
        }
    }

    @Test
    void manyMessagesOnOneConnection() throws Exception {
        start("*", "*", StandardCharsets.UTF_8, true);
        int count = 25;
        try (MllpTestClient client = new MllpTestClient(port, StandardCharsets.UTF_8)) {
            for (int i = 0; i < count; i++) {
                String ack = client.sendAndReceive(adtA01("SEQ-" + i));
                assertEquals("AA", ackCode(ack));
                assertEquals("SEQ-" + i, ackControlId(ack));
            }
        }
        assertEquals(count, factory.committed.size());
    }

    @Test
    void concurrentClientsAreAllStoredExactlyOnce() throws Exception {
        start("*", "*", StandardCharsets.UTF_8, true);
        int clients = 8;
        int perClient = 15;
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        try {
            List<Callable<List<String>>> tasks = new ArrayList<>();
            for (int c = 0; c < clients; c++) {
                final int clientNo = c;
                tasks.add(() -> {
                    List<String> acks = new ArrayList<>();
                    try (MllpTestClient client = new MllpTestClient(port, StandardCharsets.UTF_8)) {
                        for (int i = 0; i < perClient; i++) {
                            acks.add(client.sendAndReceive(adtA01("C" + clientNo + "-" + i)));
                        }
                    }
                    return acks;
                });
            }
            Set<String> ackedIds = new HashSet<>();
            for (Future<List<String>> f : pool.invokeAll(tasks)) {
                for (String ack : f.get()) {
                    assertEquals("AA", ackCode(ack));
                    ackedIds.add(ackControlId(ack));
                }
            }
            assertEquals(clients * perClient, ackedIds.size());
            assertEquals(clients * perClient, factory.committed.size());

            Set<String> storedIds = new HashSet<>();
            for (Stored s : factory.committed) {
                String content = new String(s.content, StandardCharsets.UTF_8);
                storedIds.add(content.split("\r")[0].split("\\|")[9]);   // MSH-10
            }
            assertEquals(ackedIds, storedIds);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void largeMessageIsStoredCompletely() throws Exception {
        start("*", "*", StandardCharsets.UTF_8, true);
        StringBuilder text = new StringBuilder();
        while (text.length() < 2_000_000) {
            text.append("Lorem ipsum dolor sit amet ");
        }
        String msg = oruR01("BIG-1") + "OBX|2|TX|NOTE^Note||" + text + "\r";

        String ack = send(msg);

        assertEquals("AA", ackCode(ack));
        assertEquals(1, factory.committed.size());
        assertTrue(factory.committed.get(0).content.length >= 2_000_000);
    }

    // ================================================================== character sets

    @Test
    void configuredCharsetIsUsedWhenMsh18IsMissing() throws Exception {
        start("*", "*", StandardCharsets.ISO_8859_1, true);

        String ack = send(adtA01("LATIN1"), StandardCharsets.ISO_8859_1);

        assertEquals("AA", ackCode(ack));
        String content = new String(factory.committed.get(0).content, StandardCharsets.UTF_8);
        assertTrue(content.contains("Müller^Jürg"), content);
    }

    @Test
    void msh18TakesPrecedenceOverConfiguredCharset() throws Exception {
        start("*", "*", StandardCharsets.ISO_8859_1, true);

        String msg = message("ADT^A01", "UTF8-MSH18", "2.5", "HIS", "RIH", "EKG", "EKG", "UNICODE UTF-8", "Müller^Jürg");
        String ack = send(msg, StandardCharsets.UTF_8);

        assertEquals("AA", ackCode(ack));
        String content = new String(factory.committed.get(0).content, StandardCharsets.UTF_8);
        assertTrue(content.contains("Müller^Jürg"), content);
    }

    // ================================================================== failure paths: AE, nothing stored

    @Test
    void commitFailureReturnsAeAndStoresNothing() throws Exception {
        start("*", "*", StandardCharsets.UTF_8, true);
        factory.mode = Mode.COMMIT_FAILS;

        String ack = send(adtA01("FAIL-COMMIT"));

        assertEquals("AE", ackCode(ack));
        assertEquals("FAIL-COMMIT", ackControlId(ack));
        assertEquals(0, factory.committed.size());
    }

    @Test
    void commitHangingReturnsAeAfterTimeout() throws Exception {
        start("*", "*", StandardCharsets.UTF_8, true);
        factory.mode = Mode.COMMIT_HANGS;

        long t0 = System.nanoTime();
        String ack = send(adtA01("HANG"));
        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        assertEquals("AE", ackCode(ack));
        assertEquals(0, factory.committed.size());
        assertTrue(millis < 5_000, "AE must come after the commit timeout, took " + millis + " ms");
    }

    @Test
    void writeFailureReturnsAeAndRollsBack() throws Exception {
        start("*", "*", StandardCharsets.UTF_8, true);
        factory.mode = Mode.WRITE_FAILS;

        String ack = send(adtA01("FAIL-WRITE"));

        assertEquals("AE", ackCode(ack));
        assertEquals(0, factory.committed.size());
        assertEquals(1, factory.rollbacks.get());
    }

    @Test
    void missingSessionFactoryReturnsAe() throws Exception {
        // onTrigger never called -> latch never counted down
        start("*", "*", StandardCharsets.UTF_8, false);

        long t0 = System.nanoTime();
        String ack = send(adtA01("NO-FACTORY"));
        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        assertEquals("AE", ackCode(ack));
        assertEquals(0, factory.sessions.get());
        assertTrue(millis >= SHORT_TIMEOUT_MS - 50, "must wait for the session factory, took " + millis + " ms");
    }

    @Test
    void sessionFactoryRemovedWhileStoppingReturnsAe() throws Exception {
        AtomicReference<ProcessSessionFactory> ref = new AtomicReference<>();   // stays null
        CountDownLatch latch = new CountDownLatch(1);
        latch.countDown();
        NifiHL7HapiServer server = new NifiHL7HapiServer(ref, latch, SUCCESS, SHORT_TIMEOUT_MS, SHORT_TIMEOUT_MS);
        servers.add(server);
        server.startServer(port, "*", "*", StandardCharsets.UTF_8);

        assertEquals("AE", ackCode(send(adtA01("GONE"))));
    }

    @Test
    void serverRecoversAfterFailures() throws Exception {
        start("*", "*", StandardCharsets.UTF_8, true);
        try (MllpTestClient client = new MllpTestClient(port, StandardCharsets.UTF_8)) {
            factory.mode = Mode.COMMIT_FAILS;
            assertEquals("AE", ackCode(client.sendAndReceive(adtA01("R-1"))));
            factory.mode = Mode.WRITE_FAILS;
            assertEquals("AE", ackCode(client.sendAndReceive(adtA01("R-2"))));
            factory.mode = Mode.OK;
            assertEquals("AA", ackCode(client.sendAndReceive(adtA01("R-1"))));   // sender retry
        }
        assertEquals(1, factory.committed.size());
    }

    // ================================================================== filter

    @Test
    void messageTypeFilterRejectsOtherTypes() throws Exception {
        start("ADT", "A01", StandardCharsets.UTF_8, true);
        try (MllpTestClient client = new MllpTestClient(port, StandardCharsets.UTF_8)) {
            String rejected = client.sendAndReceive(oruR01("ORU-1"));
            assertNotNull(rejected, "a NAK is expected, not a dropped connection");
            assertNotEquals("AA", ackCode(rejected));

            assertEquals("AA", ackCode(client.sendAndReceive(adtA01("ADT-1"))));
        }
        assertEquals(1, factory.committed.size());
        assertEquals("ADT^A01", factory.committed.get(0).attributes.get(NifiHL7HapiServer.MSG_TYPE));
    }

    // ================================================================== broken input / connections

    @Test
    void garbageIsNeverAcknowledgedPositivelyAndServerStaysUsable() throws Exception {
        start("*", "*", StandardCharsets.UTF_8, true);

        try (MllpTestClient client = new MllpTestClient(port, StandardCharsets.UTF_8)) {
            client.send("THIS IS NOT HL7");
            String response = readQuietly(client);
            assertNotEquals("AA", ackCode(response));
        }
        assertEquals(0, factory.committed.size());

        // Unknown version: HAPI may accept it generically or reject it - but never AA without storing.
        try (MllpTestClient client = new MllpTestClient(port, StandardCharsets.UTF_8)) {
            client.send("MSH|^~\\&|HIS|RIH|EKG|EKG|20261110||ADT^A01|UNKNOWN-V|P|9.9\rPID|1\r");
            String response = readQuietly(client);
            boolean stored = factory.committed.stream()
                    .anyMatch(s -> new String(s.content, StandardCharsets.UTF_8).contains("|UNKNOWN-V|"));
            assertEquals("AA".equals(ackCode(response)), stored, "AA must be sent if and only if stored");
        }

        int before = factory.committed.size();
        assertEquals("AA", ackCode(send(adtA01("AFTER-GARBAGE"))));
        assertEquals(before + 1, factory.committed.size());
    }

    @Test
    void abortedConnectionsDoNotBreakServer() throws Exception {
        start("*", "*", StandardCharsets.UTF_8, true);

        // connect and close without sending anything
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("localhost", port), 5_000);
        }
        // close in the middle of a frame
        try (MllpTestClient client = new MllpTestClient(port, StandardCharsets.UTF_8)) {
            byte[] partial = ("MSH|^~\\&|HIS|RIH|EKG|EKG|20261110||ADT^A01|PART").getBytes(StandardCharsets.UTF_8);
            byte[] frame = new byte[partial.length + 1];
            frame[0] = START_BLOCK;
            System.arraycopy(partial, 0, frame, 1, partial.length);
            client.sendRaw(frame);
        }
        // client sends a message and disconnects without reading the ACK
        try (MllpTestClient client = new MllpTestClient(port, StandardCharsets.UTF_8)) {
            client.send(adtA01("NO-READ"));
        }

        assertEquals("AA", ackCode(send(adtA01("STILL-ALIVE"))));
        assertTrue(factory.committed.stream().anyMatch(s -> new String(s.content, StandardCharsets.UTF_8).contains("|STILL-ALIVE|")));
        assertTrue(factory.committed.stream().noneMatch(s -> new String(s.content, StandardCharsets.UTF_8).contains("|PART")));
    }

    // ================================================================== lifecycle

    @Test
    void startFailsIfPortIsAlreadyInUse() throws Exception {
        try (ServerSocket blocker = new ServerSocket()) {
            blocker.bind(new InetSocketAddress(port));
            NifiHL7HapiServer server = newServer(new AtomicReference<>(), new CountDownLatch(1));
            ProcessException e = assertThrows(ProcessException.class,
                    () -> server.startServer(port, "*", "*", StandardCharsets.UTF_8));
            assertTrue(e.getMessage().contains(String.valueOf(port)), e.getMessage());
        }
    }

    @Test
    void stopReleasesPortAndRestartWorks() throws Exception {
        NifiHL7HapiServer first = start("*", "*", StandardCharsets.UTF_8, true);
        assertEquals("AA", ackCode(send(adtA01("FIRST"))));

        first.stopServer();
        servers.remove(first);
        assertTrue(isPortFree(port), "port must be released after stop");
        assertThrows(IOException.class, () -> new MllpTestClient(port, StandardCharsets.UTF_8).close());

        start("*", "*", StandardCharsets.UTF_8, true);
        assertEquals("AA", ackCode(send(adtA01("SECOND"))));
        assertEquals(2, factory.committed.size());
    }

    @Test
    void stopWithOpenClientConnection() throws Exception {
        NifiHL7HapiServer server = start("*", "*", StandardCharsets.UTF_8, true);
        try (MllpTestClient client = new MllpTestClient(port, StandardCharsets.UTF_8)) {
            assertEquals("AA", ackCode(client.sendAndReceive(adtA01("OPEN"))));
            assertTimeoutPreemptively(java.time.Duration.ofSeconds(30), server::stopServer);
            servers.remove(server);
        }
        start("*", "*", StandardCharsets.UTF_8, true);
        assertEquals("AA", ackCode(send(adtA01("AFTER-RESTART"))));
    }

    // ================================================================== helpers

    private NifiHL7HapiServer newServer(AtomicReference<ProcessSessionFactory> ref, CountDownLatch latch) {
        NifiHL7HapiServer server = new NifiHL7HapiServer(ref, latch, SUCCESS, SHORT_TIMEOUT_MS, SHORT_TIMEOUT_MS);
        servers.add(server);
        return server;
    }

    private NifiHL7HapiServer start(String type, String trigger, Charset charset, boolean provideSessionFactory) {
        AtomicReference<ProcessSessionFactory> ref = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        NifiHL7HapiServer server = newServer(ref, latch);
        server.startServer(port, type, trigger, charset);
        if (provideSessionFactory) {       // what HL7Server.onTrigger does
            ref.set(factory);
            latch.countDown();
        }
        return server;
    }

    private String send(String msg) throws IOException {
        return send(msg, StandardCharsets.UTF_8);
    }

    private String send(String msg, Charset charset) throws IOException {
        try (MllpTestClient client = new MllpTestClient(port, charset)) {
            return client.sendAndReceive(msg);
        }
    }

    private static String readQuietly(MllpTestClient client) {
        try {
            client.setReadTimeout(5_000);
            return client.readFrame();
        } catch (IOException e) {
            return null;   // server closed the connection: acceptable, as long as nothing was stored
        }
    }

    // ================================================================== simulated NiFi session

    enum Mode {OK, COMMIT_FAILS, COMMIT_HANGS, WRITE_FAILS}

    static final class Stored {
        final Map<String, String> attributes = new HashMap<>();
        byte[] content = new byte[0];
    }

    /**
     * Thread-safe stand-in for NiFi's session factory. Unsupported session methods throw,
     * so the test also fails if the server starts using API it does not handle correctly.
     */
    static final class FakeSessionFactory implements ProcessSessionFactory {
        volatile Mode mode = Mode.OK;
        final List<Stored> committed = new CopyOnWriteArrayList<>();
        final AtomicInteger rollbacks = new AtomicInteger();
        final AtomicInteger sessions = new AtomicInteger();
        final AtomicInteger provenanceReceives = new AtomicInteger();

        @Override
        public ProcessSession createSession() {
            sessions.incrementAndGet();
            return (ProcessSession) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{ProcessSession.class}, new SessionHandler());
        }

        private final class SessionHandler implements InvocationHandler {
            private final List<Stored> pending = Collections.synchronizedList(new ArrayList<>());

            @Override
            @SuppressWarnings("unchecked")
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                Object objectResult = objectMethod(proxy, method, args);
                if (objectResult != null) return objectResult;
                int argc = args == null ? 0 : args.length;
                switch (method.getName()) {
                    case "create":
                        if (argc == 0) return newFlowFile();
                        break;
                    case "write":
                        if (argc == 2 && args[1] instanceof OutputStreamCallback) {
                            if (mode == Mode.WRITE_FAILS) throw new ProcessException("simulated write failure");
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ((OutputStreamCallback) args[1]).process(bos);
                            stored(args[0]).content = bos.toByteArray();
                            return args[0];
                        }
                        break;
                    case "putAllAttributes":
                        stored(args[0]).attributes.putAll((Map<String, String>) args[1]);
                        return args[0];
                    case "putAttribute":
                        stored(args[0]).attributes.put((String) args[1], (String) args[2]);
                        return args[0];
                    case "getProvenanceReporter":
                        return provenanceReporter();
                    case "transfer":
                        if (argc == 2 && args[0] instanceof FlowFile) {
                            pending.add(stored(args[0]));
                            return null;
                        }
                        break;
                    case "rollback":
                        pending.clear();
                        rollbacks.incrementAndGet();
                        return null;
                    case "commitAsync":
                        if (argc == 2) {
                            Runnable onSuccess = (Runnable) args[0];
                            Consumer<Throwable> onFailure = (Consumer<Throwable>) args[1];
                            switch (mode) {
                                case COMMIT_FAILS -> onFailure.accept(new IOException("simulated commit failure"));
                                case COMMIT_HANGS -> { /* never completes */ }
                                default -> {
                                    committed.addAll(pending);
                                    pending.clear();
                                    onSuccess.run();
                                }
                            }
                            return null;
                        }
                        break;
                    default:
                        break;
                }
                throw new UnsupportedOperationException("not simulated: " + method);
            }
        }

        private ProvenanceReporter provenanceReporter() {
            return (ProvenanceReporter) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{ProvenanceReporter.class}, (proxy, method, args) -> {
                        Object o = objectMethod(proxy, method, args);
                        if (o != null) return o;
                        if ("receive".equals(method.getName())) provenanceReceives.incrementAndGet();
                        return defaultValue(method.getReturnType());
                    });
        }
    }

    private static final class FlowFileHandler implements InvocationHandler {
        final Stored stored = new Stored();

        FlowFileHandler() {
            String uuid = UUID.randomUUID().toString();
            stored.attributes.put("uuid", uuid);
            stored.attributes.put("filename", uuid);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            Object o = objectMethod(proxy, method, args);
            if (o != null) return o;
            return switch (method.getName()) {
                case "getAttribute" -> stored.attributes.get((String) args[0]);
                case "getAttributes" -> Collections.unmodifiableMap(stored.attributes);
                case "getSize" -> (long) stored.content.length;
                default -> defaultValue(method.getReturnType());
            };
        }
    }

    private static FlowFile newFlowFile() {
        return (FlowFile) Proxy.newProxyInstance(NifiHL7HapiServerTest.class.getClassLoader(),
                new Class<?>[]{FlowFile.class}, new FlowFileHandler());
    }

    private static Stored stored(Object flowFile) {
        return ((FlowFileHandler) Proxy.getInvocationHandler(flowFile)).stored;
    }

    /** equals/hashCode/toString for proxies; null if the method is not one of them. */
    private static Object objectMethod(Object proxy, Method method, Object[] args) {
        return switch (method.getName()) {
            case "equals" -> (args != null && args.length == 1) ? (Object) (proxy == args[0]) : null;
            case "hashCode" -> (args == null || args.length == 0) ? (Object) System.identityHashCode(proxy) : null;
            case "toString" -> (args == null || args.length == 0) ? "Proxy@" + System.identityHashCode(proxy) : null;
            default -> null;
        };
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive() || type == void.class) return null;
        if (type == boolean.class) return false;
        if (type == long.class) return 0L;
        if (type == int.class) return 0;
        if (type == double.class) return 0d;
        if (type == float.class) return 0f;
        if (type == short.class) return (short) 0;
        if (type == byte.class) return (byte) 0;
        if (type == char.class) return (char) 0;
        return null;
    }
}
