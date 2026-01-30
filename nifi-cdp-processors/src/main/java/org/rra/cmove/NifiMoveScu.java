package org.rra.cmove;

import lombok.extern.slf4j.Slf4j;
import org.dcm4che3.data.*;
import org.dcm4che3.net.*;
import org.dcm4che3.net.pdu.AAssociateRQ;
import org.dcm4che3.net.pdu.ExtendedNegotiation;
import org.dcm4che3.net.pdu.PresentationContext;
import org.dcm4che3.util.TagUtils;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.*;

@Slf4j
public class NifiMoveScu extends Device {
    private static final int[] DEF_IN_FILTER = {Tag.SOPInstanceUID, Tag.StudyInstanceUID, Tag.SeriesInstanceUID};
    private static final String[] IVR_LE_FIRST = {
            UID.ImplicitVRLittleEndian,
            UID.ExplicitVRLittleEndian,
            UID.ExplicitVRBigEndian
    };
    private final ApplicationEntity ae;
    private final Connection conn = new Connection();
    private final Connection remote = new Connection();
    private final AAssociateRQ rq = new AAssociateRQ();
    private final Attributes keys = new Attributes();
    private int priority;
    private String destination;
    private InformationModel model;
    private final int[] inFilter = DEF_IN_FILTER;
    private Association as;
    private int cancelAfter;
    private boolean releaseEager;
    private ScheduledFuture<?> scheduledCancel;

    private static final int CONNECT_TIMEOUT  = 5_000;
    private static final int REQUEST_TIMEOUT  = 10_000;
    private static final int RESPONSE_TIMEOUT = 300_000;
    private static final int ACCEPT_TIMEOUT   = 20_000; //
    private static final int RELEASE_TIMEOUT  = 5_000;
    private static final int SEND_TIMEOUT     = 30_000;

    public NifiMoveScu(String host, int port, String callingAET, String calledAET, String moveAET) {
        super("movescu");
        ae = new ApplicationEntity(callingAET);
        addConnection(conn);
        addApplicationEntity(ae);
        ae.addConnection(conn);
        // configureConnect
        rq.setCalledAET(calledAET);
        remote.setHostname(host);
        remote.setPort(port);
        //configure
        conn.setReceivePDULength(Connection.DEF_MAX_PDU_LENGTH);
        conn.setSendPDULength(Connection.DEF_MAX_PDU_LENGTH);
        conn.setMaxOpsInvoked(0);
        conn.setMaxOpsPerformed(0);
        conn.setPackPDV(true);

        conn.setConnectTimeout(CONNECT_TIMEOUT);
        conn.setRequestTimeout(REQUEST_TIMEOUT);
        conn.setResponseTimeout(RESPONSE_TIMEOUT);
        conn.setAcceptTimeout(ACCEPT_TIMEOUT);
        conn.setReleaseTimeout(RELEASE_TIMEOUT);
        conn.setSendTimeout(SEND_TIMEOUT);

        conn.setStoreTimeout(0);

        remote.setTlsProtocols(conn.getTlsProtocols());
        remote.setTlsCipherSuites(conn.getTlsCipherSuites());
        //configureServiceClass
        setInformationModel(InformationModel.StudyRoot, IVR_LE_FIRST, false);
        setPriority(Priority.NORMAL);
        setDestination(moveAET);
        setCancelAfter(0);
        setReleaseEager(false);

    }

    public void moveSeries(String studyIUID, String seriesIUID, IMoveComplete moveComplete, IMoveHasErrors errorHandler) throws Exception {
        keys.clear();
        addKey(Tag.StudyInstanceUID, studyIUID);
        addKey(Tag.SeriesInstanceUID, seriesIUID);
        addRetrieveLevel("SERIES");
        runMoveScu(moveComplete, errorHandler);
    }

    public void moveStudy(String studyIUID, IMoveComplete moveComplete, IMoveHasErrors errorHandler) throws Exception {
        keys.clear();
        addKey(Tag.StudyInstanceUID, studyIUID);
        addRetrieveLevel("STUDY");
        runMoveScu(moveComplete, errorHandler);
    }

    private void runMoveScu(IMoveComplete moveComplete, IMoveHasErrors errorHandler) throws Exception {
        try {
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            setExecutor(executorService);
            setScheduledExecutor(scheduledExecutorService);
            try {
                open();
                retrieve(moveComplete, errorHandler);
            } finally {
                close();
                executorService.shutdown();
                executorService.awaitTermination(10, TimeUnit.SECONDS);
                scheduledExecutorService.shutdown();
                scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS);
            }
        } catch (Exception exception) {
            log.info("Error: " + exception.getMessage());
            throw exception;
        }
    }

    private final void setPriority(int priority) {
        this.priority = priority;
    }

    private void setCancelAfter(int cancelAfter) {
        this.cancelAfter = cancelAfter;
    }

    private void setReleaseEager(boolean releaseEager) {
        this.releaseEager = releaseEager;
    }

    private final void setInformationModel(InformationModel model, String[] tss, boolean relational) {
        this.model = model;
        rq.addPresentationContext(new PresentationContext(1, model.cuid, tss));
        if (relational) rq.addExtendedNegotiation(new ExtendedNegotiation(model.cuid, new byte[]{1}));
        if (model.level != null) addRetrieveLevel(model.level);
    }

    private void addRetrieveLevel(String s) {
        keys.setString(Tag.QueryRetrieveLevel, VR.CS, s);
    }

    private final void setDestination(String destination) {
        this.destination = destination;
    }

    private void addKey(int tag, String... ss) {
        VR vr = ElementDictionary.vrOf(tag, keys.getPrivateCreator(tag));
        keys.setString(tag, vr, ss);
    }

    private void open() throws IOException, InterruptedException, IncompatibleConnectionException, GeneralSecurityException {
        as = ae.connect(conn, remote, rq);
    }

    private void close() throws IOException, InterruptedException {
        if (scheduledCancel != null && releaseEager) { // release by scheduler thread
            return;
        }
        if (as != null && as.isReadyForDataTransfer()) {
            if (!releaseEager) {
                as.waitForOutstandingRSP();
            }
            as.release();
        }
    }


    private void retrieve(final IMoveComplete moveComplete, final IMoveHasErrors erroHandler) throws Exception {
        retrieve2(keys, moveComplete, erroHandler);
    }

    private void retrieve2(Attributes keys,
                           final IMoveComplete moveComplete,
                           final IMoveHasErrors errorHandler) throws Exception {

        final CountDownLatch done = new CountDownLatch(1);
        MoveDimseRSPHandler rspHandler = new MoveDimseRSPHandler(as.nextMessageID(), errorHandler, moveComplete) {
            @Override
            public void onDimseRSP(Association as, Attributes cmd, Attributes data) {
                super.onDimseRSP(as, cmd, data);
                int status = cmd.getInt(Tag.Status, -1);

                // Nur bei finaler Antwort freigeben
                if (!Status.isPending(status)) {
                    done.countDown();
                }
            }
        };
        as.cmove(model.cuid, priority, keys, null, destination, rspHandler);
        if (cancelAfter > 0) {
            scheduledCancel = schedule(() -> {
                try {
                    rspHandler.cancel(as);
                    if (releaseEager) {
                        as.release();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }, cancelAfter, TimeUnit.MILLISECONDS);
        }
        // WICHTIG: auf final warten (LAN: z.B. 10 min)
        boolean finished = done.await(10, TimeUnit.MINUTES);
        if (!finished) {
            try {
                rspHandler.cancel(as);
            } catch (Exception ignore) {
            }
            if (errorHandler != null) {
                errorHandler.moveHasError(Status.ProcessingFailure, "C-MOVE timed out (app deadline)");
            }
            throw new InterruptedIOException("C-MOVE timed out (app deadline)");
        }
    }

     private enum InformationModel {
        PatientRoot(UID.PatientRootQueryRetrieveInformationModelMove, "STUDY"),
        StudyRoot(UID.StudyRootQueryRetrieveInformationModelMove, "STUDY"),
        PatientStudyOnly(UID.PatientStudyOnlyQueryRetrieveInformationModelMove, "STUDY"),
        CompositeInstanceRoot(UID.CompositeInstanceRootRetrieveMove, "IMAGE"),
        HangingProtocol(UID.HangingProtocolInformationModelMove, null),
        ColorPalette(UID.ColorPaletteQueryRetrieveInformationModelMove, null);

        final String cuid;
        final String level;

        InformationModel(String cuid, String level) {
            this.cuid = cuid;
            this.level = level;
        }
    }

    private class MoveDimseRSPHandler extends DimseRSPHandler {
        final IMoveHasErrors errorHandler;
        final IMoveComplete moveComplete;
        public MoveDimseRSPHandler(int msgId, IMoveHasErrors erroHandler, IMoveComplete moveComplete) {
            super(msgId);
            this.errorHandler = erroHandler;
            this.moveComplete = moveComplete;
        }

        @Override
        public void onDimseRSP(Association as, Attributes cmd, Attributes data) {
            super.onDimseRSP(as, cmd, data);
            int status = cmd.getInt(Tag.Status, -1);
            if (!Status.isPending(status) && (status != Status.Success)) {
                log.error("C-Move has Errors. (status={})", TagUtils.toHexString(status));
                if (null != errorHandler){
                    try {
                        errorHandler.moveHasError(status, "C-Move has Errors. (status="+ TagUtils.toHexString(status)+ ")");
                    } catch (Exception e) {
                        e.printStackTrace();

                    }
                }
            }else if (status == Status.Success){
                moveComplete.moveComplete(keys.getString(Tag.StudyInstanceUID), keys.getString(Tag.SeriesInstanceUID));
            }
        }
    }

}
