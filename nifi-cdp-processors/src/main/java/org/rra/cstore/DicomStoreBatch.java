package org.rra.cstore;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.net.ApplicationEntity;
import org.dcm4che3.net.Association;
import org.dcm4che3.net.Connection;
import org.dcm4che3.net.Device;
import org.dcm4che3.net.DimseRSP;
import org.dcm4che3.net.IncompatibleConnectionException;
import org.dcm4che3.net.InputStreamDataWriter;
import org.dcm4che3.net.Priority;
import org.dcm4che3.net.pdu.AAssociateRJ;
import org.dcm4che3.net.pdu.AAssociateRQ;
import org.dcm4che3.net.pdu.PresentationContext;
import org.rra.dcmconfig.DcmConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Sends several DICOM instances to one Storage SCP over a single
 * association and reports the outcome of every instance separately.
 *
 * <p>Input is a Part-10 file or a bare data set without file meta
 * information, as GetScu and the pseudonymizer of dcm4nifi write it. For a
 * bare data set SOP Class and Instance UID come from the data set and the
 * transfer syntax from its encoding (implicit/explicit VR); encapsulated
 * (compressed) pixel data additionally needs the transfer syntax as
 * {@link Instance#transferSyntaxHint()}, otherwise the instance fails.</p>
 *
 * <p>An instance counts as stored only if the SCP answered the C-STORE with
 * Success or a Warning status. Everything else is a failure with an error
 * code, never silently skipped:</p>
 * <ul>
 *   <li>{@link #INVALID_DICOM}: neither a readable Part-10 header nor a data
 *       set with SOP Class/Instance UID and a known transfer syntax</li>
 *   <li>{@link #CONNECTION_FAILED}: SCP not reachable, timeout</li>
 *   <li>{@link #ASSOCIATION_REJECTED}: SCP rejected the association (e.g. unknown Called AE)</li>
 *   <li>{@link #NO_PRESENTATION_CONTEXT}: SOP class not accepted</li>
 *   <li>{@link #TRANSFER_SYNTAX_NOT_ACCEPTED}: SOP class accepted, transfer syntax of the file not</li>
 *   <li>{@link #STORE_REJECTED}: C-STORE answered with a failure status</li>
 *   <li>{@link #ASSOCIATION_ABORTED}: association lost while sending; instances after
 *       the failed one are not sent</li>
 * </ul>
 *
 * <p>No transcoding: the data set is streamed in the transfer syntax of the
 * file, without loading pixel data into memory. Independent of NiFi so it
 * can be tested against a real SCP.</p>
 */
public final class DicomStoreBatch {

    private static final Logger log = LoggerFactory.getLogger(DicomStoreBatch.class);

    public static final String INVALID_DICOM = "INVALID_DICOM";
    public static final String CONNECTION_FAILED = "CONNECTION_FAILED";
    public static final String ASSOCIATION_REJECTED = "ASSOCIATION_REJECTED";
    public static final String NO_PRESENTATION_CONTEXT = "NO_PRESENTATION_CONTEXT";
    public static final String TRANSFER_SYNTAX_NOT_ACCEPTED = "TRANSFER_SYNTAX_NOT_ACCEPTED";
    public static final String STORE_REJECTED = "STORE_REJECTED";
    public static final String ASSOCIATION_ABORTED = "ASSOCIATION_ABORTED";

    /** Presentation context IDs are odd numbers 1..255: at most 128 per association. */
    static final int MAX_PRESENTATION_CONTEXTS = 128;

    /** Destination of one association. */
    public record Destination(String host, int port, String callingAet, String calledAet) {
        public Destination {
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("host must not be empty");
            }
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("invalid port: " + port);
            }
            if (callingAet == null || callingAet.isBlank() || callingAet.length() > 16) {
                throw new IllegalArgumentException("invalid Calling AE Title: " + callingAet);
            }
            if (calledAet == null || calledAet.isBlank() || calledAet.length() > 16) {
                throw new IllegalArgumentException("invalid Called AE Title: " + calledAet);
            }
            host = host.trim();
            callingAet = callingAet.trim();
            calledAet = calledAet.trim();
        }

        @Override
        public String toString() {
            return calledAet + "@" + host + ":" + port + " (calling " + callingAet + ")";
        }
    }

    /** One instance to send; {@link #open()} returns a new stream each time. */
    @FunctionalInterface
    public interface Instance {
        InputStream open() throws IOException;

        /**
         * Transfer syntax of a data set without file meta information, e.g.
         * the FlowFile attribute TransferSyntax set by GetScu. Only used for
         * encapsulated pixel data; the encoding of native data is read from
         * the data itself (the pseudonymizer re-encodes it).
         */
        default String transferSyntaxHint() {
            return null;
        }
    }

    public enum Outcome { SUCCESS, WARNING, FAILED }

    /**
     * @param status         DIMSE status of the C-STORE-RSP, -1 if none was received
     * @param errorCode      null unless FAILED
     * @param sopInstanceUid from the file meta information, null if unreadable
     */
    public record Result(Outcome outcome, int status, String errorCode, String message, String sopInstanceUid) {

        public boolean stored() {
            return outcome != Outcome.FAILED;
        }

        static Result stored(int status, String comment, String iuid) {
            return new Result(classify(status), status, null, comment, iuid);
        }

        static Result failed(String code, String message, String iuid) {
            return new Result(Outcome.FAILED, -1, code, message, iuid);
        }

        static Result failed(String code, int status, String message, String iuid) {
            return new Result(Outcome.FAILED, status, code, message, iuid);
        }
    }

    /**
     * SOP class, instance and transfer syntax of one instance.
     *
     * @param part10 true: file meta information is skipped when sending;
     *               false: the stream is sent as it is (bare data set)
     */
    record Header(int index, String cuid, String iuid, String tsuid, boolean part10) {

        Header(int index, String cuid, String iuid, String tsuid) {
            this(index, cuid, iuid, tsuid, true);
        }
    }

    /** Transfer syntaxes without encapsulated pixel data. */
    private static final Set<String> NATIVE = Set.of(
            UID.ImplicitVRLittleEndian, UID.ExplicitVRLittleEndian, UID.ExplicitVRBigEndian,
            UID.DeflatedExplicitVRLittleEndian);

    private final DcmConfig cfg;

    public DicomStoreBatch(DcmConfig cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
    }

    /**
     * Success (0000) and the warning statuses of PS3.7 (0001, 0107, 0116,
     * Bxxx) count as stored; every other status is a failure.
     */
    static Outcome classify(int status) {
        if (status == 0x0000) {
            return Outcome.SUCCESS;
        }
        if (status == 0x0001 || status == 0x0107 || status == 0x0116 || (status & 0xF000) == 0xB000) {
            return Outcome.WARNING;
        }
        return Outcome.FAILED;
    }

    static String hex(int status) {
        return String.format("%04X", status & 0xFFFF);
    }

    /**
     * Sends all instances to {@code destination}; the result list has the
     * same order and size as {@code instances}.
     */
    public List<Result> send(Destination destination, List<? extends Instance> instances) {
        Objects.requireNonNull(destination, "destination");
        Result[] results = new Result[instances.size()];

        List<Header> headers = new ArrayList<>();
        for (int i = 0; i < instances.size(); i++) {
            try {
                headers.add(readHeader(i, instances.get(i)));
            } catch (InvalidDicomException e) {
                results[i] = Result.failed(INVALID_DICOM, e.getMessage(), null);
            }
        }

        for (List<Header> chunk : chunks(headers)) {
            sendChunk(destination, instances, chunk, results);
        }

        for (int i = 0; i < results.length; i++) {
            if (results[i] == null) {
                // Defensive: every instance must have an outcome
                results[i] = Result.failed(ASSOCIATION_ABORTED, "not sent", null);
            }
        }
        return Arrays.asList(results);
    }

    /** Splits so that no association needs more than 128 presentation contexts. */
    static List<List<Header>> chunks(List<Header> headers) {
        List<List<Header>> chunks = new ArrayList<>();
        List<Header> current = new ArrayList<>();
        Set<String> contexts = new LinkedHashSet<>();
        for (Header header : headers) {
            String context = header.cuid() + "|" + header.tsuid();
            if (!contexts.contains(context) && contexts.size() == MAX_PRESENTATION_CONTEXTS) {
                chunks.add(current);
                current = new ArrayList<>();
                contexts = new LinkedHashSet<>();
            }
            contexts.add(context);
            current.add(header);
        }
        if (!current.isEmpty()) {
            chunks.add(current);
        }
        return chunks;
    }

    private static final class InvalidDicomException extends Exception {
        InvalidDicomException(String message) {
            super(message);
        }
    }

    static Header readHeader(int index, Instance instance) throws InvalidDicomException {
        try (InputStream in = instance.open();
             DicomInputStream dis = new DicomInputStream(in)) {
            Attributes fmi = dis.readFileMetaInformation();
            if (fmi == null) {
                return bareDataSet(index, dis, instance.transferSyntaxHint());
            }
            String cuid = fmi.getString(Tag.MediaStorageSOPClassUID);
            String iuid = fmi.getString(Tag.MediaStorageSOPInstanceUID);
            String tsuid = fmi.getString(Tag.TransferSyntaxUID);
            if (cuid == null || iuid == null || tsuid == null) {
                throw new InvalidDicomException(
                        "File meta information without SOP Class, SOP Instance or Transfer Syntax UID");
            }
            return new Header(index, cuid, iuid, tsuid);
        } catch (IOException | RuntimeException e) {
            throw new InvalidDicomException("Not a readable DICOM file: " + describe(e));
        }
    }

    /** Data set without file meta information (GetScu, pseudonymizer). */
    private static Header bareDataSet(int index, DicomInputStream dis, String hint)
            throws IOException, InvalidDicomException {
        dis.setIncludeBulkData(DicomInputStream.IncludeBulkData.NO);
        Attributes ds = dis.readDatasetUntilPixelData();
        String cuid = ds.getString(Tag.SOPClassUID);
        String iuid = ds.getString(Tag.SOPInstanceUID);
        if (cuid == null || iuid == null) {
            throw new InvalidDicomException(
                    "No DICOM file meta information (Part-10 header) and no SOP Class/Instance UID in the data set");
        }
        String encoding = dis.getTransferSyntax();
        boolean encapsulated = dis.tag() == Tag.PixelData && dis.length() == -1;
        if (!encapsulated) {
            return new Header(index, cuid, iuid, encoding, false);
        }
        String tsuid = hint == null ? "" : hint.trim();
        if (tsuid.isEmpty() || NATIVE.contains(tsuid)) {
            throw new InvalidDicomException("Compressed pixel data without file meta information: "
                    + "transfer syntax unknown (TransferSyntax attribute: " + (tsuid.isEmpty() ? "none" : tsuid) + ")");
        }
        if (!UID.ExplicitVRLittleEndian.equals(encoding)) {
            throw new InvalidDicomException("Compressed pixel data in a data set encoded as " + encoding);
        }
        return new Header(index, cuid, iuid, tsuid, false);
    }

    private void sendChunk(Destination destination, List<? extends Instance> instances,
                           List<Header> chunk, Result[] results) {

        Device device = new Device("dcm4nifi-storescu");
        Connection conn = new Connection();
        configure(conn, cfg);
        device.addConnection(conn);
        ApplicationEntity ae = new ApplicationEntity(destination.callingAet());
        device.addApplicationEntity(ae);
        ae.addConnection(conn);

        Connection remote = new Connection();
        remote.setHostname(destination.host());
        remote.setPort(destination.port());

        AAssociateRQ rq = new AAssociateRQ();
        rq.setCalledAET(destination.calledAet());
        Set<String> contexts = new LinkedHashSet<>();
        for (Header header : chunk) {
            if (contexts.add(header.cuid() + "|" + header.tsuid())) {
                rq.addPresentationContext(new PresentationContext(
                        2 * contexts.size() - 1, header.cuid(), header.tsuid()));
            }
        }

        ExecutorService executor = Executors.newCachedThreadPool();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        device.setExecutor(executor);
        device.setScheduledExecutor(scheduler);

        Association as = null;
        try {
            try {
                as = ae.connect(remote, rq);
            } catch (AAssociateRJ e) {
                failAll(chunk, results, ASSOCIATION_REJECTED,
                        "Association rejected by " + destination + ": " + describe(e));
                return;
            } catch (IOException | IncompatibleConnectionException | GeneralSecurityException e) {
                failAll(chunk, results, CONNECTION_FAILED,
                        "Cannot connect to " + destination + ": " + describe(e));
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failAll(chunk, results, CONNECTION_FAILED, "Interrupted while connecting to " + destination);
                return;
            }

            int stored = 0;
            for (int i = 0; i < chunk.size(); i++) {
                Header header = chunk.get(i);
                if (!as.isReadyForDataTransfer()) {
                    failAll(chunk.subList(i, chunk.size()), results, ASSOCIATION_ABORTED,
                            "Not sent: association to " + destination + " closed");
                    break;
                }
                Result result = sendOne(as, header, instances.get(header.index()));
                results[header.index()] = result;
                if (result.stored()) {
                    stored++;
                } else if (ASSOCIATION_ABORTED.equals(result.errorCode())) {
                    failAll(chunk.subList(i + 1, chunk.size()), results, ASSOCIATION_ABORTED,
                            "Not sent: association to " + destination + " lost at an earlier instance");
                    break;
                }
            }
            log.info("C-STORE to {}: {} of {} instances stored", destination, stored, chunk.size());
        } finally {
            close(as);
            executor.shutdown();
            scheduler.shutdown();
        }
    }

    private Result sendOne(Association as, Header header, Instance instance) {
        Set<String> accepted = as.getTransferSyntaxesFor(header.cuid());
        if (accepted == null || accepted.isEmpty()) {
            return Result.failed(NO_PRESENTATION_CONTEXT,
                    "SOP Class " + header.cuid() + " not accepted by " + as.getRemoteAET(), header.iuid());
        }
        if (!accepted.contains(header.tsuid())) {
            return Result.failed(TRANSFER_SYNTAX_NOT_ACCEPTED,
                    "Transfer Syntax " + header.tsuid() + " not accepted by " + as.getRemoteAET()
                            + " for SOP Class " + header.cuid(), header.iuid());
        }

        try (InputStream in = instance.open()) {
            InputStream data = in;
            if (header.part10()) {
                DicomInputStream dis = new DicomInputStream(in);
                dis.readFileMetaInformation();
                data = dis;
            }
            DimseRSP rsp = as.cstore(header.cuid(), header.iuid(), Priority.NORMAL,
                    new InputStreamDataWriter(data), header.tsuid());
            if (!rsp.next()) {
                return Result.failed(ASSOCIATION_ABORTED, "No C-STORE response", header.iuid());
            }
            Attributes cmd = rsp.getCommand();
            int status = cmd.getInt(Tag.Status, -1);
            String comment = cmd.getString(Tag.ErrorComment);
            if (classify(status) == Outcome.FAILED) {
                return Result.failed(STORE_REJECTED, status,
                        "C-STORE status " + hex(status) + (comment != null ? ": " + comment : ""),
                        header.iuid());
            }
            return Result.stored(status, comment, header.iuid());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Result.failed(ASSOCIATION_ABORTED, "Interrupted while sending", header.iuid());
        } catch (IOException | RuntimeException e) {
            // Includes reading the FlowFile and a lost association; the
            // association is not trusted any further.
            if (as.isReadyForDataTransfer()) {
                as.abort();
            }
            return Result.failed(ASSOCIATION_ABORTED, "C-STORE failed: " + describe(e), header.iuid());
        }
    }

    private static void failAll(List<Header> headers, Result[] results, String code, String message) {
        for (Header header : headers) {
            results[header.index()] = Result.failed(code, message, header.iuid());
        }
    }

    private static void close(Association as) {
        if (as == null) {
            return;
        }
        try {
            if (as.isReadyForDataTransfer()) {
                as.release();
            }
            as.waitForSocketClose();
        } catch (IOException e) {
            log.debug("Release of association failed: {}", e.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Failures of a batch for the log: per error code the count and the
     * first message, e.g. {@code ASSOCIATION_REJECTED x50 (A-ASSOCIATE-RJ ...)}.
     * Messages are technical (AE titles, UIDs, exceptions), no patient data.
     *
     * @return null if every instance was stored
     */
    public static String failureSummary(List<Result> results) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        Map<String, String> firstMessage = new LinkedHashMap<>();
        for (Result result : results) {
            if (result.stored()) {
                continue;
            }
            String code = result.errorCode() == null ? "UNKNOWN" : result.errorCode();
            counts.merge(code, 1, Integer::sum);
            if (result.message() != null && !result.message().isBlank()) {
                firstMessage.putIfAbsent(code, result.message());
            }
        }
        if (counts.isEmpty()) {
            return null;
        }
        List<String> parts = new ArrayList<>();
        counts.forEach((code, count) -> {
            String message = firstMessage.get(code);
            parts.add(code + " x" + count + (message == null ? "" : " (" + abbreviate(message, 300) + ")"));
        });
        return String.join("; ", parts);
    }

    private static String abbreviate(String text, int max) {
        return text.length() <= max ? text : text.substring(0, max) + "...";
    }

    private static String describe(Throwable e) {
        String message = e.getMessage();
        return message == null || message.isBlank()
                ? e.getClass().getSimpleName()
                : e.getClass().getSimpleName() + ": " + message;
    }

    /** Same connection settings as the other SCUs of dcm4nifi. */
    static void configure(Connection conn, DcmConfig cfg) {
        conn.setReceivePDULength(Connection.DEF_MAX_PDU_LENGTH);
        conn.setSendPDULength(Connection.DEF_MAX_PDU_LENGTH);
        // One C-STORE at a time: each response belongs to exactly one FlowFile
        conn.setMaxOpsInvoked(1);
        conn.setMaxOpsPerformed(1);
        conn.setPackPDV(!cfg.NOT_PACK_PDV);
        conn.setConnectTimeout(cfg.CONNECT_TIMEOUT);
        conn.setRequestTimeout(cfg.REQUEST_TIMEOUT);
        conn.setAcceptTimeout(cfg.ACCEPT_TIMEOUT);
        conn.setReleaseTimeout(cfg.RELEASE_TIMEOUT);
        conn.setSendTimeout(cfg.SEND_TIMEOUT);
        conn.setStoreTimeout(cfg.STORE_TIMEOUT);
        conn.setResponseTimeout(cfg.RESPONSE_TIMEOUT);
        conn.setIdleTimeout(cfg.IDLE_TIMEOUT);
        conn.setSocketCloseDelay(Connection.DEF_SOCKETDELAY);
        conn.setSendBufferSize(cfg.SND_BUFFER);
        conn.setReceiveBufferSize(cfg.RCV_BUFFER);
        conn.setTcpNoDelay(!cfg.TCP_DELAY);
    }
}
