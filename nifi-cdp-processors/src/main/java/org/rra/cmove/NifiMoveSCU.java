package org.rra.cmove;

import lombok.extern.slf4j.Slf4j;
import org.dcm4che3.data.*;
import org.dcm4che3.net.*;
import org.dcm4che3.net.pdu.AAssociateRQ;
import org.dcm4che3.net.pdu.ExtendedNegotiation;
import org.dcm4che3.net.pdu.PresentationContext;
import org.dcm4che3.util.TagUtils;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.*;

@Slf4j
public class NifiMoveSCU extends Device {
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
    private int[] inFilter = DEF_IN_FILTER;
    private Association as;
    private int cancelAfter;
    private boolean releaseEager;
    private ScheduledFuture<?> scheduledCancel;
    private ScheduledFuture<?> scheduledOnError;

    public NifiMoveSCU(String host, int port, String callingAET, String calledAET, String moveAET) {
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
        conn.setConnectTimeout(2000);
        conn.setRequestTimeout(100);
        conn.setAcceptTimeout(100);
        conn.setReleaseTimeout(100);
        conn.setSendTimeout(100);
        conn.setStoreTimeout(100);
        conn.setResponseTimeout(100);
        remote.setTlsProtocols(conn.getTlsProtocols());
        remote.setTlsCipherSuites(conn.getTlsCipherSuites());
        //configureServiceClass
        setInformationModel(InformationModel.StudyRoot, IVR_LE_FIRST, false);
        setPriority(Priority.NORMAL);
        setDestination(moveAET);
        setCancelAfter(0);
        setReleaseEager(false);

    }

    public void moveSeries(String studyIUID, String seriesIUID) throws Exception {
        addKey(Tag.StudyInstanceUID, studyIUID);
        addKey(Tag.SeriesInstanceUID, seriesIUID);
        addRetrieveLevel("SERIES");
        runMoveScu();
    }
    public void moveStudy(String studyIUID) throws Exception {
        addKey(Tag.StudyInstanceUID, studyIUID);
        addRetrieveLevel("STUDY");
        runMoveScu();
    }

    private void runMoveScu() throws Exception {
        try {
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            setExecutor(executorService);
            setScheduledExecutor(scheduledExecutorService);
            try {
                open();
                retrieve(keys);
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

//    private final void setInputFilter(int[] inFilter) {
//        this.inFilter = inFilter;
//    }

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

//    private void retrieve(File f) throws IOException, InterruptedException {
//        Attributes attrs = new Attributes();
//        DicomInputStream dis = null;
//        try {
//            attrs.addSelected(new DicomInputStream(f).readDataset(), inFilter);
//        } finally {
//            SafeClose.close(dis);
//        }
//        attrs.addAll(keys);
//        retrieve(attrs);
//    }

   /* private void retrieve(final MoveComplete moveComplete, final MoveHasErrors erroHandler, final Response responseHandler) throws IOException, InterruptedException {
        retrieve(this.keys, moveComplete, erroHandler, responseHandler);
    }
*/
    private final class MoveDimseRSPHandler extends DimseRSPHandler {
        public MoveDimseRSPHandler(int msgId) {
            super(msgId);
        }

        @Override
        public void onDimseRSP(Association as, Attributes cmd, Attributes data) {
            super.onDimseRSP(as, cmd, data);
            int status = cmd.getInt(Tag.Status, -1);
            if (!Status.isPending(status) && (status != Status.Success)) {
                log.debug("##################### cmd = {}", cmd);
                log.info("C-Move has Errors. (status={})", TagUtils.toHexString(status));
            }else if (status == Status.Success){
                log.info("C-Move Complete.");
            }
        }
    }
    
    private void retrieve(Attributes keys) throws IOException, InterruptedException {
        /*final DimseRSPHandler rspHandler = new DimseRSPHandler(as.nextMessageID()) {
            @Override
            public void onDimseRSP(Association as, Attributes cmd, Attributes data) {
                super.onDimseRSP(as, cmd, data);
            }
        };
        */
        MoveDimseRSPHandler rspHandler = new MoveDimseRSPHandler(as.nextMessageID());

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
}
