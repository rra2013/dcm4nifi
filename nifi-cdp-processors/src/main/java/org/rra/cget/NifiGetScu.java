package org.rra.cget;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.net.*;
import org.dcm4che3.net.pdu.AAssociateRQ;
import org.dcm4che3.net.pdu.ExtendedNegotiation;
import org.dcm4che3.net.pdu.PresentationContext;
import org.dcm4che3.net.pdu.RoleSelection;
import org.dcm4che3.net.service.DicomServiceRegistry;
import org.dcm4che3.util.SafeClose;
import org.dcm4che3.util.StreamUtils;
import org.dcm4che3.util.StringUtils;
import org.rra.dcmconfig.DcmConfig;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class NifiGetScu {
    public static final DcmConfig.LEVEL STUDY_LEVEL = DcmConfig.LEVEL.STUDY;
    public static final DcmConfig.LEVEL SERIES_LEVEL = DcmConfig.LEVEL.SERIES;

    private static final String[] IVR_LE_FIRST = {
            UID.ImplicitVRLittleEndian,
            UID.ExplicitVRLittleEndian,
            UID.ExplicitVRBigEndian
    };
    private final Device device = new Device("getscu");
    private final ApplicationEntity applicationEntity;
    private final Connection conn = new Connection();
    private final Connection remote = new Connection();
    private final AAssociateRQ rq = new AAssociateRQ();
    private final Attributes keys = new Attributes();
    private final int cancelAfter;
    private final int priority;
    private final ProcessSession session;
    private final Relationship relationship;
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor();
    private InformationModel model;
    private Association as;


    public NifiGetScu(String host, int port, String callingAet, String calledAet, ProcessSession session, Relationship relationship, DcmConfig cfg) {
        this.relationship = relationship;
        this.session = session;

        device.addConnection(conn);
        applicationEntity = new ApplicationEntity(callingAet);
        device.addApplicationEntity(applicationEntity);
        applicationEntity.addConnection(conn);
        device.setDimseRQHandler(createServiceRegistry());

        rq.setCalledAET(calledAet);
        remote.setHostname(host);
        remote.setPort(port);
        remote.setHttpProxy(null);
        configure(conn, cfg);
        remote.setTlsProtocols(conn.getTlsProtocols());
        remote.setTlsCipherSuites(conn.getTlsCipherSuites());

        this.cancelAfter = 0;
        this.priority = Priority.NORMAL;
        setInformationModel(InformationModel.StudyRoot, IVR_LE_FIRST, false);
        device.setExecutor(executorService);
        device.setScheduledExecutor(scheduledExecutorService);

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

    private static void configureServiceClass(NifiGetScu that) throws IOException {
        Properties p = loadProperties("resource:store-tcs.properties", null);
        Set<Map.Entry<Object, Object>> entrySet = p.entrySet();
        for (Map.Entry<Object, Object> entry : entrySet)
            configureStorageSOPClass(that, (String) entry.getKey(), (String) entry.getValue());
    }

    private static void configureStorageSOPClass(NifiGetScu that, String cuid, String tsuids0) {
        String[] tsuids1 = StringUtils.split(tsuids0, ';');
        for (String tsuids2 : tsuids1) {
            that.addOfferedStorageSOPClass(toUID(cuid), toUIDs(tsuids2));
        }
    }

    private static String[] toUIDs(String s) {
        if (s.equals("*"))
            return new String[]{"*"};

        String[] uids = StringUtils.split(s, ',');
        for (int i = 0; i < uids.length; i++)
            uids[i] = toUID(uids[i]);
        return uids;
    }

    private static String toUID(String uid) {
        uid = uid.trim();
        return (uid.equals("*") || Character.isDigit(uid.charAt(0)))
                ? uid
                : UID.forName(uid);
    }

    private static Properties loadProperties(String url, Properties p)
            throws IOException {
        if (p == null)
            p = new Properties();
        InputStream in = StreamUtils.openFileOrURL(url);
        try {
            p.load(in);
        } finally {
            SafeClose.close(in);
        }
        return p;
    }

    public void getOnStudyLevel(String studyIUID) throws Exception {
        doGet(STUDY_LEVEL, studyIUID, null);

    }

    public void getOnSeriesLevel(String studyIUID, String seriesIUID) throws Exception {
        doGet(SERIES_LEVEL, studyIUID, seriesIUID);

    }

    private void doGet(DcmConfig.LEVEL level, String studyIUID, String seriesIUID) throws Exception {
        if (level == STUDY_LEVEL) {
            addLevel(STUDY_LEVEL.toString());
            this.keys.setString(Tag.StudyInstanceUID, VR.UI, studyIUID);
        } else if (level == SERIES_LEVEL) {
            addLevel(SERIES_LEVEL.toString());
            this.keys.setString(Tag.StudyInstanceUID, VR.UI, studyIUID);
            this.keys.setString(Tag.SeriesInstanceUID, VR.UI, seriesIUID);
        } else {
            throw new IOException("Wrong level");
        }
        try {
            configureServiceClass(this);
            try {
                open();
                retrieve(this.keys);
            } finally {
                close();
                executorService.shutdown();
                executorService.awaitTermination(10, TimeUnit.SECONDS);
                scheduledExecutorService.shutdown();
                scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            throw e;
        }
    }

    private void setInformationModel(InformationModel model, String[] tss,
                                     boolean relational) {
        this.model = model;
        rq.addPresentationContext(new PresentationContext(1, model.cuid, tss));
        if (relational)
            rq.addExtendedNegotiation(new ExtendedNegotiation(model.cuid, new byte[]{1}));
        if (model.level != null)
            addLevel(model.level);
    }

    private void addLevel(String s) {
        keys.setString(Tag.QueryRetrieveLevel, VR.CS, s);
    }

    public void addOfferedStorageSOPClass(String cuid, String... tsuids) {
        if (!rq.containsPresentationContextFor(cuid))
            rq.addRoleSelection(new RoleSelection(cuid, false, true));
        rq.addPresentationContext(new PresentationContext(
                2 * rq.getNumberOfPresentationContexts() + 1, cuid, tsuids));
    }

    private void open() throws IOException, InterruptedException, IncompatibleConnectionException, GeneralSecurityException {
        as = applicationEntity.connect(conn, remote, rq);
    }

    private void retrieve(Attributes keys) throws IOException, InterruptedException {
        final DimseRSPHandler rspHandler = new DimseRSPHandler(as.nextMessageID()) {

            @Override
            public void onDimseRSP(Association as, Attributes cmd,
                                   Attributes data) {
                super.onDimseRSP(as, cmd, data);
            }
        };

        retrieve(keys, rspHandler);
        if (cancelAfter > 0) {
            device.schedule(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        rspHandler.cancel(as);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                            },
                    cancelAfter,
                    TimeUnit.MILLISECONDS);
        }
    }

    private void retrieve(Attributes keys, DimseRSPHandler rspHandler) throws IOException, InterruptedException {
        as.cget(model.cuid, priority, keys, null, rspHandler);
    }

    private void close() throws IOException, InterruptedException {
        if (as != null && as.isReadyForDataTransfer()) {
            as.waitForOutstandingRSP();
            as.release();
        }
    }

    private DicomServiceRegistry createServiceRegistry() {
        DicomServiceRegistry serviceRegistry = new DicomServiceRegistry();
        serviceRegistry.addDicomService(new NifiCGetCSotreSCP(session, relationship));
        return serviceRegistry;
    }

    private enum InformationModel {
        PatientRoot(UID.PatientRootQueryRetrieveInformationModelGet, "STUDY"),
        StudyRoot(UID.StudyRootQueryRetrieveInformationModelGet, "STUDY"),
        PatientStudyOnly(UID.PatientStudyOnlyQueryRetrieveInformationModelGet, "STUDY"),
        CompositeInstanceRoot(UID.CompositeInstanceRootRetrieveGet, "IMAGE"),
        WithoutBulkData(UID.CompositeInstanceRetrieveWithoutBulkDataGet, null),
        HangingProtocol(UID.HangingProtocolInformationModelGet, null),
        ColorPalette(UID.ColorPaletteQueryRetrieveInformationModelGet, null);

        final String cuid;
        final String level;

        InformationModel(String cuid, String level) {
            this.cuid = cuid;
            this.level = level;
        }
    }


}
