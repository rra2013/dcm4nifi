package org.rra.cstore;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomInputStream.IncludeBulkData;
import org.dcm4che3.net.*;
import org.dcm4che3.net.pdu.AAssociateRQ;
import org.dcm4che3.net.pdu.PresentationContext;
import org.dcm4che3.util.SafeClose;
import org.dcm4che3.util.TagUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.rra.cstore.DcmStoreSCUConfig.*;


@Slf4j
public class DcmStoreSCU {
    private final Device device = new Device("storescu");
    private final ApplicationEntity ae;
    private final AAssociateRQ rq = new AAssociateRQ();
    private final RelatedGeneralSOPClasses relSOPClasses = new RelatedGeneralSOPClasses();
    private final Connection remote = new Connection();
    private final Connection conn = new Connection();
    private final Attributes attrs = new Attributes();
    private final String uidSuffix = null;
    private final String tmpPrefix = "storescu-";
    private final List<StreamMetaInfo> streamMetaInfos;
    private boolean relExtNeg;
    private int priority;
    private String tmpSuffix;
    private File tmpDir;
    private File tmpFile;
    private Association as;
    private long totalSize;
    private int filesScanned = 0;
    private int filesSent = 0;
    private final RSPHandlerFactory rspHandlerFactory = new RSPHandlerFactory() {

        @Override
        public DimseRSPHandler createDimseRSPHandler() {

            return new DimseRSPHandler(as.nextMessageID()) {

                @Override
                public void onDimseRSP(Association as, Attributes cmd, Attributes data) {
                    super.onDimseRSP(as, cmd, data);
                    DcmStoreSCU.this.onCStoreRSP(cmd);
                }
            };
        }
    };

    public DcmStoreSCU(String host, int port, String callingAET, String calledAET) throws Exception {
        //Do only echo
        this(host, port, callingAET, calledAET, null);
        doEcho();
    }

    public DcmStoreSCU(String host, int port, String callingAET, String calledAET, InputStream inputStream) throws Exception {

        this.device.addConnection(conn);
        this.ae = new ApplicationEntity(callingAET);
        this.device.addApplicationEntity(ae);
        this.ae.addConnection(conn);
        // Request
        this.rq.setCalledAET(calledAET);
        this.rq.addPresentationContext(new PresentationContext(1, UID.Verification, UID.ImplicitVRLittleEndian));
        // Connection
        this.remote.setHostname(host);
        this.remote.setPort(port);
        this.remote.setHttpProxy(null);
        configure(this.conn);
        this.remote.setTlsProtocols(this.conn.getTlsProtocols());
        this.remote.setTlsCipherSuites(this.conn.getTlsCipherSuites());

        if (null == inputStream) {
            this.streamMetaInfos = null;
            return;
        }

        this.streamMetaInfos = new ArrayList<>();
        addStreamToSendList(inputStream, new CallbackStream() {
            @Override
            public boolean dicomFile(Attributes fmi, long dsPos, Attributes ds) throws Exception {

                if (!addFileStream(streamMetaInfos, ds, dsPos, fmi)) return false;

                return true;
            }
        });
        sendStreamMetaInfos();
    }

    private static void configure(Connection conn) {
        // -- max-pdulen-rcv
        // -- max-pdulen-snd
        // 16378 by default
        conn.setReceivePDULength(Connection.DEF_MAX_PDU_LENGTH);
        conn.setSendPDULength(Connection.DEF_MAX_PDU_LENGTH);
        /*
         * do not use asynchronous mode;
         * equivalent to
         * --max-ops-invoked=1 and
         * --max-ops-performed=1
         */
        if (NOT_ASYNC) {
            conn.setMaxOpsInvoked(1);
            conn.setMaxOpsPerformed(1);
        } else {
            conn.setMaxOpsInvoked(0);
            conn.setMaxOpsPerformed(0);
        }
        conn.setPackPDV(!NOT_PACK_PDV);
        conn.setConnectTimeout(CONNECT_TIMEOUT);
        conn.setRequestTimeout(REQUEST_TIMEOUT);
        conn.setAcceptTimeout(ACCEPT_TIMEOUT);
        conn.setReleaseTimeout(RELEASE_TIMEOUT);
        conn.setSendTimeout(SEND_TIMEOUT);
        conn.setStoreTimeout(STORE_TIMEOUT);
        conn.setResponseTimeout(RESPONSE_TIMEOUT);

        conn.setIdleTimeout(IDLE_TIMEOUT);
        conn.setSocketCloseDelay(Connection.DEF_SOCKETDELAY);
        conn.setSendBufferSize(SND_BUFFER);
        conn.setReceiveBufferSize(RCV_BUFFER);
        conn.setTcpNoDelay(!TCP_DELAY);
    }

    /*private static void addFileToSendList(File f, Callback scb) {
        DicomInputStream in = null;
        try {
            in = new DicomInputStream(f);
            in.setIncludeBulkData(IncludeBulkData.NO);
            Attributes fmi = in.readFileMetaInformation();
            long dsPos = in.getPosition();
            Attributes ds = in.readDatasetUntilPixelData();
            if (fmi == null || !fmi.containsValue(Tag.TransferSyntaxUID) || !fmi.containsValue(Tag.MediaStorageSOPClassUID) || !fmi.containsValue(Tag.MediaStorageSOPInstanceUID))
                fmi = ds.createFileMetaInformation(in.getTransferSyntax());
            boolean b = scb.dicomFile(f, fmi, dsPos, ds);
            log.debug(b ? "'File adden to Send List" : "Could Not Add File, not a DICOM File");
        } catch (Exception e) {
            log.info("Failed to scan file " + f + ": " + e.getMessage());
            e.printStackTrace(System.out);
        } finally {
            SafeClose.close(in);
        }
    }*/

    private static void addStreamToSendList(InputStream inputStream, CallbackStream scb) {
        DicomInputStream in = null;
        try {
            in = new DicomInputStream(inputStream);
            in.setIncludeBulkData(IncludeBulkData.URI);
            Attributes fmi = in.readFileMetaInformation();
            long dsPos = in.getPosition();
            //Attributes ds = in.readDatasetUntilPixelData();
            Attributes ds = in.readDataset();
            byte[] bytes = ds.getBytes(Tag.PixelData);
            if (null != bytes) log.info("Count of pixel data {}", bytes.length);
            if (fmi == null || !fmi.containsValue(Tag.TransferSyntaxUID) || !fmi.containsValue(Tag.MediaStorageSOPClassUID) || !fmi.containsValue(Tag.MediaStorageSOPInstanceUID))
                fmi = ds.createFileMetaInformation(in.getTransferSyntax());
            boolean b = scb.dicomFile(fmi, dsPos, ds);
            log.debug(b ? "'Stream added to Send List" : "Could Not Add Stream, not a DICOM Stream");
        } catch (Exception e) {
            e.printStackTrace(System.out);
        } finally {
            SafeClose.close(in);
        }
    }

    private void doEcho() throws IncompatibleConnectionException, GeneralSecurityException, IOException, InterruptedException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        device.setExecutor(executorService);
        device.setScheduledExecutor(scheduledExecutorService);
        long t1 = 0, t2 = 0;
        try {
            t1 = System.currentTimeMillis();
            open();
            log.info("Connected to {} in {}ms.", this.as.getRemoteAET(), t2 - t1);
            echo();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        } finally {
            try {
                close();
                log.info("Connection closed. OK.");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            executorService.shutdown();
            scheduledExecutorService.shutdown();
        }

    }

    private void sendStreamMetaInfos() throws Exception {
        if (null == this.streamMetaInfos) {
            throw new IllegalArgumentException("Nothing to send. Only C-Echo mode.");
        }
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        device.setExecutor(executorService);
        device.setScheduledExecutor(scheduledExecutorService);
        long t1 = 0, t2 = 0;
        try {
            t1 = System.currentTimeMillis();
            open();
            t2 = System.currentTimeMillis();
            log.info("Connected to {} in {}ms.", this.as.getRemoteAET(), t2 - t1);
            t1 = System.currentTimeMillis();
            sendStreams(this.streamMetaInfos);
            t2 = System.currentTimeMillis();
        } catch (Exception ex) {
            log.error(ex.getMessage());
            throw ex;
        } finally {
            try {
                close();
                log.info("Connection closed. OK.");
            } catch (Exception ex) {
                log.error(ex.getMessage());
            }
            executorService.shutdown();
            scheduledExecutorService.shutdown();
        }
        /*if (this.filesScanned > 0) {
            float s = (t2 - t1) / 1000F;
            float mb = this.totalSize / 1048576F;
            log.info("Sent {} objects (={}MB) in {}s (={}MB/s)", this.filesSent, mb, s, mb / s);
        } else {
            log.info("No DICOM Files could be found.");
        }*/
    }

   /* private List<FileMetaInfo> addFilesToSendList(List<File> files) {
        final List<FileMetaInfo> fileInfos = new ArrayList<>();
        if (null != files) for (File dcmFile : files) {
            addFileToSendList(dcmFile, new Callback() {
                @Override
                public boolean dicomFile(File f, Attributes fmi, long dsPos, Attributes ds) {
                    if (!addFile(fileInfos, f, dsPos, fmi)) return false;

                    filesScanned++;
                    return true;
                }
            });
        }
        return fileInfos;
    }*/

    /*private boolean addFile(List<FileMetaInfo> fileInfos, File f, long endFmi, Attributes fmi) {
        String cuid = fmi.getString(Tag.MediaStorageSOPClassUID);
        String iuid = fmi.getString(Tag.MediaStorageSOPInstanceUID);
        String ts = fmi.getString(Tag.TransferSyntaxUID);
        if (cuid == null || iuid == null) {
            return false;
        }
        // Add File to Infos
        fileInfos.add(new FileMetaInfo(iuid, cuid, ts, endFmi, f, false));

        if (rq.containsPresentationContextFor(cuid, ts)) return true;

        if (!rq.containsPresentationContextFor(cuid)) {
            if (relExtNeg) rq.addCommonExtendedNegotiation(relSOPClasses.getCommonExtendedNegotiation(cuid));
            if (!ts.equals(UID.ExplicitVRLittleEndian))
                rq.addPresentationContext(new PresentationContext(rq.getNumberOfPresentationContexts() * 2 + 1, cuid, UID.ExplicitVRLittleEndian));
            if (!ts.equals(UID.ImplicitVRLittleEndian))
                rq.addPresentationContext(new PresentationContext(rq.getNumberOfPresentationContexts() * 2 + 1, cuid, UID.ImplicitVRLittleEndian));
        }
        rq.addPresentationContext(new PresentationContext(rq.getNumberOfPresentationContexts() * 2 + 1, cuid, ts));
        return true;
    }*/
    private boolean addFileStream(List<StreamMetaInfo> fileInfos, Attributes data, long endFmi, Attributes fmi) {
        String cuid = fmi.getString(Tag.MediaStorageSOPClassUID);
        String iuid = fmi.getString(Tag.MediaStorageSOPInstanceUID);
        String ts = fmi.getString(Tag.TransferSyntaxUID);
        if (cuid == null || iuid == null) {
            return false;
        }
        // Add File to Infos
        fileInfos.add(new StreamMetaInfo(iuid, cuid, ts, endFmi, data, false));

        if (rq.containsPresentationContextFor(cuid, ts)) return true;

        if (!rq.containsPresentationContextFor(cuid)) {
            if (relExtNeg) rq.addCommonExtendedNegotiation(relSOPClasses.getCommonExtendedNegotiation(cuid));
            if (!ts.equals(UID.ExplicitVRLittleEndian))
                rq.addPresentationContext(new PresentationContext(rq.getNumberOfPresentationContexts() * 2 + 1, cuid, UID.ExplicitVRLittleEndian));
            if (!ts.equals(UID.ImplicitVRLittleEndian))
                rq.addPresentationContext(new PresentationContext(rq.getNumberOfPresentationContexts() * 2 + 1, cuid, UID.ImplicitVRLittleEndian));
        }
        rq.addPresentationContext(new PresentationContext(rq.getNumberOfPresentationContexts() * 2 + 1, cuid, ts));
        return true;
    }

    private void sendStreams(List<StreamMetaInfo> streamInfos) {
        for (StreamMetaInfo info : streamInfos) {
            while (as.isReadyForDataTransfer() && !info.done) {
                try {
                    send(info.getData(),info.getCuid(), info.getIuid(), info.getTs());
                    info.setDone(true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            try {
                as.waitForOutstandingRSP();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void echo() throws IOException, InterruptedException {
        as.cecho().next();
    }

    private void send(final Attributes data, String cuid, String iuid, String transferSyntax) {
        String ts = selectTransferSyntax(cuid, transferSyntax);
        if (uidSuffix == null && attrs.isEmpty() && ts.equals(transferSyntax)) {
            /*try {
                inputStream.skip(fmiEndPos);
                InputStreamDataWriter data = new InputStreamDataWriter(inputStream);
                as.cstore(cuid, iuid, priority, data, ts, rspHandlerFactory.createDimseRSPHandler());
            } catch (Exception e) {

            }*/
            try {
                as.cstore(cuid, iuid, priority, new DataWriterAdapter(data), ts, rspHandlerFactory.createDimseRSPHandler());
            } catch (Exception e) {

            }
        }
        else {
//            DicomInputStream in = new DicomInputStream(f);
//            try {
//                in.setIncludeBulkData(IncludeBulkData.URI);
//                Attributes data = in.readDataset();
//                if (CLIUtils.updateAttributes(data, attrs, uidSuffix)) iuid = data.getString(Tag.SOPInstanceUID);
//                if (!ts.equals(filets)) {
//                    Decompressor.decompress(data, filets);
//                }
//                as.cstore(cuid, iuid, priority, new DataWriterAdapter(data), ts, rspHandlerFactory.createDimseRSPHandler(f));
//            } finally {
//                SafeClose.close(in);
//            }
        }
    }

    private String selectTransferSyntax(String cuid, String filets) {
        Set<String> tss = as.getTransferSyntaxesFor(cuid);
        if (tss.contains(filets)) return filets;

        if (tss.contains(UID.ExplicitVRLittleEndian)) return UID.ExplicitVRLittleEndian;

        return UID.ImplicitVRLittleEndian;
    }

    private void close() throws IOException, InterruptedException {
        if (as != null) {
            if (as.isReadyForDataTransfer()) as.release();
            as.waitForSocketClose();
        }
    }

    private void open() throws IOException, InterruptedException, IncompatibleConnectionException, GeneralSecurityException {
        as = ae.connect(remote, rq);
    }

    private void onCStoreRSP(Attributes cmd) {
        int status = cmd.getInt(Tag.Status, -1);
        switch (status) {
            case Status.Success:
                totalSize += 1;
                ++filesSent;
                log.info("Send File OK. {}", 1);
                break;
            case Status.CoercionOfDataElements:
            case Status.ElementsDiscarded:
            case Status.DataSetDoesNotMatchSOPClassWarning:
                totalSize += 0;
                ++filesSent;
                log.info("WARNING: Received C-STORE-RSP with Status {}H for {}", TagUtils.shortToHexString(status));
                log.info(cmd.toString());
                break;
            default:
                log.info("ERROR: Received C-STORE-RSP with Status {}H for {}", TagUtils.shortToHexString(status));
                log.error(cmd.toString());
        }
    }


    private interface RSPHandlerFactory {

        DimseRSPHandler createDimseRSPHandler();
    }

    private interface CallbackStream {
        boolean dicomFile(Attributes fmi, long dsPos, Attributes ds) throws Exception;
    }

    public interface SendingCallback {
        void onMessage(String message, long time);
    }

    @Getter
    @AllArgsConstructor
    private class StreamMetaInfo {
        final String iuid;
        final String cuid;
        final String ts;
        final long endFmi;
        final Attributes data;
        @Setter
        boolean done;
    }
}
