package org.rra.cstore;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.net.ApplicationEntity;
import org.dcm4che3.net.Association;
import org.dcm4che3.net.Connection;
import org.dcm4che3.net.Device;
import org.dcm4che3.net.PDVInputStream;
import org.dcm4che3.net.TransferCapability;
import org.dcm4che3.net.pdu.PresentationContext;
import org.dcm4che3.net.service.BasicCEchoSCP;
import org.dcm4che3.net.service.BasicCStoreSCP;
import org.dcm4che3.net.service.DicomServiceException;
import org.dcm4che3.net.service.DicomServiceRegistry;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Real dcm4che Storage SCP on localhost for tests: accepts the configured
 * SOP classes and transfer syntaxes, records what it received and can answer
 * single instances with an error status or abort the association.
 */
public final class TestStoreScp implements AutoCloseable {

    public record Received(String calledAet, String callingAet, String iuid, String tsuid, long bytes) {}

    private final Device device = new Device("test-storescp");
    private final ApplicationEntity ae;
    private final Connection conn = new Connection();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final List<Received> received = Collections.synchronizedList(new ArrayList<>());
    private final Set<Association> associations = Collections.newSetFromMap(Collections.synchronizedMap(new IdentityHashMap<>()));
    private final Map<String, Integer> statusFor = new ConcurrentHashMap<>();
    private final Set<String> abortOn = ConcurrentHashMap.newKeySet();
    private final int port;

    /**
     * @param aeTitle    Called AE the SCP answers to ("*" = any)
     * @param sopClasses accepted SOP classes ("*" = all)
     * @param tsuids     accepted transfer syntaxes ("*" = all)
     */
    public TestStoreScp(String aeTitle, String[] sopClasses, String... tsuids) throws Exception {
        this.port = freePort();
        this.ae = new ApplicationEntity(aeTitle);
        device.addConnection(conn);
        device.addApplicationEntity(ae);
        ae.setAssociationAcceptor(true);
        ae.addConnection(conn);
        conn.setHostname("127.0.0.1");
        conn.setPort(port);
        for (String cuid : sopClasses) {
            ae.addTransferCapability(new TransferCapability(null, cuid, TransferCapability.Role.SCP, tsuids));
        }
        ae.addTransferCapability(new TransferCapability(null, UID.Verification, TransferCapability.Role.SCP,
                UID.ImplicitVRLittleEndian));

        DicomServiceRegistry registry = new DicomServiceRegistry();
        registry.addDicomService(new BasicCEchoSCP());
        registry.addDicomService(new BasicCStoreSCP("*") {
            @Override
            protected void store(Association as, PresentationContext pc, Attributes rq,
                                 PDVInputStream data, Attributes rsp) throws IOException {
                associations.add(as);
                String iuid = rq.getString(Tag.AffectedSOPInstanceUID);
                if (abortOn.contains(iuid)) {
                    as.abort();
                    return;
                }
                long bytes = data.transferTo(java.io.OutputStream.nullOutputStream());
                Integer status = statusFor.get(iuid);
                if (status != null) {
                    throw new DicomServiceException(status, "test error for " + iuid);
                }
                received.add(new Received(as.getCalledAET(), as.getCallingAET(), iuid, pc.getTransferSyntax(), bytes));
            }
        });
        device.setDimseRQHandler(registry);
        device.setExecutor(executor);
        device.setScheduledExecutor(scheduler);
        device.bindConnections();
    }

    public static TestStoreScp acceptingAll(String aeTitle) throws Exception {
        return new TestStoreScp(aeTitle, new String[] {"*"}, "*");
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    public int port() {
        return port;
    }

    /** Answers the C-STORE of this instance with {@code status} (and stores nothing). */
    public void failWith(String iuid, int status) {
        statusFor.put(iuid, status);
    }

    /** Aborts the association when this instance arrives. */
    public void abortOn(String iuid) {
        abortOn.add(iuid);
    }

    public List<Received> received() {
        synchronized (received) {
            return List.copyOf(received);
        }
    }

    /** Distinct associations that carried at least one C-STORE. */
    public int associationCount() {
        return associations.size();
    }

    @Override
    public void close() {
        device.unbindConnections();
        executor.shutdownNow();
        scheduler.shutdownNow();
    }
}
