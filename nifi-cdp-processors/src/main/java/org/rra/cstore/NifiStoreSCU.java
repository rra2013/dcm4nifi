package org.rra.cstore;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.processor.exception.ProcessException;
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
import org.rra.dcmconfig.DcmConfig;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


@Slf4j
public class NifiStoreSCU {
    private final Device device = new Device("storescu");
    private final ApplicationEntity ae;
    private final AAssociateRQ rq = new AAssociateRQ();
    private final RelatedGeneralSOPClasses relSOPClasses = new RelatedGeneralSOPClasses();
    private final Connection remote = new Connection();
    private final Connection conn = new Connection();
    private final Attributes attrs = new Attributes();
    private final String uidSuffix = null;
    private final List<StreamMetaInfo> streamMetaInfos;
    private final RSPHandlerFactory rspHandlerFactory = new RSPHandlerFactory() {

        @Override
        public DimseRSPHandler createDimseRSPHandler() {

            return new DimseRSPHandler(as.nextMessageID()) {

                @Override
                public void onDimseRSP(Association as, Attributes cmd, Attributes data) {
                    super.onDimseRSP(as, cmd, data);
                    NifiStoreSCU.this.onCStoreRSP(cmd);
                }
            };
        }
    };
    //enableSOPClassRelationshipExtNeg
    private final boolean relExtNeg = false;
    private final int priority = Priority.NORMAL;
    private Association as;

    public NifiStoreSCU(String host, int port, String callingAET, String calledAET, InputStream inputStream, DcmConfig cfg) throws ProcessException {

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
        configure(this.conn, cfg);
        this.remote.setTlsProtocols(this.conn.getTlsProtocols());
        this.remote.setTlsCipherSuites(this.conn.getTlsCipherSuites());

        if (null == inputStream) {
            this.streamMetaInfos = null;
            return;
        }

        this.streamMetaInfos = new ArrayList<>();
        addStreamToSendList(inputStream, new CallbackStream() {
            @Override
            public boolean dicomObject(Attributes fmi, long dsPos, Attributes ds) {
                return addFileStream(streamMetaInfos, ds, dsPos, fmi);
            }
        });
        try {
            sendStreamMetaInfos();
        } catch (Exception e) {
            throw new ProcessException("Nothing to send.", e);
        }
    }

    private static void configure(Connection conn, DcmConfig cfg) {
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
        if (cfg.NOT_ASYNC) {
            conn.setMaxOpsInvoked(1);
            conn.setMaxOpsPerformed(1);
        } else {
            conn.setMaxOpsInvoked(0);
            conn.setMaxOpsPerformed(0);
        }
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
            boolean b = scb.dicomObject(fmi, dsPos, ds);
            log.debug(b ? "'Stream added to Send List" : "Could Not Add Stream, not a DICOM Stream");
        } catch (Exception e) {
            e.printStackTrace(System.out);
        } finally {
            SafeClose.close(in);
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
            log.info("Send to to {} in {}ms.", this.as.getRemoteAET(), t2 - t1);
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
    }

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
                    send(info.getData(), info.getCuid(), info.getIuid(), info.getTs());
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
            try {
                as.cstore(cuid, iuid, priority, new DataWriterAdapter(data), ts, rspHandlerFactory.createDimseRSPHandler());
            } catch (Exception e) {

            }
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
                log.info("Send File OK. {}", 1);
                break;
            case Status.CoercionOfDataElements:
            case Status.ElementsDiscarded:
            case Status.DataSetDoesNotMatchSOPClassWarning:
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
        boolean dicomObject(Attributes fmi, long dsPos, Attributes ds) throws Exception;
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
