package org.rra.cfind;

import lombok.extern.slf4j.Slf4j;
import org.dcm4che3.data.*;
import org.dcm4che3.net.*;
import org.dcm4che3.net.pdu.AAssociateRQ;
import org.dcm4che3.net.pdu.ExtendedNegotiation;
import org.dcm4che3.net.pdu.PresentationContext;
import org.dcm4che3.util.SafeClose;
import org.dcm4che3.util.StringUtils;

import javax.xml.transform.Templates;
import javax.xml.transform.sax.SAXTransformerFactory;
import java.io.*;
import java.security.GeneralSecurityException;
import java.text.DecimalFormat;
import java.util.EnumSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.rra.cfind.NifiFindScu.InformationModel.StudyRoot;
import static org.rra.cfind.NifiFindScuConfig.*;

@Slf4j
public class NifiFindScu {

    private static final String[] IVR_LE_FIRST = {UID.ImplicitVRLittleEndian, UID.ExplicitVRLittleEndian, UID.ExplicitVRBigEndian};
    public static int QUERY_LEVEL_PATIENT_STUDY = 0xAA;
    public static int QUERY_LEVEL_SERIES = 0xAB;
    public static int QUERY_LEVEL_IMAGE = 0xAC;
    private static SAXTransformerFactory saxtf;
    private final Device device = new Device("findscu");
    private final ApplicationEntity applicationEntity;
    private final Connection conn = new Connection();
    private final Connection remote = new Connection();
    private final AAssociateRQ rq = new AAssociateRQ();
    private final Attributes keys = new Attributes();
    private final AtomicInteger totNumMatches = new AtomicInteger();
    String[] optVals_StudyLevel = {"PatientID", "PatientName", "IssuerOfPatientID", "PatientBirthDate", "PatientSex",
            // Study Attributes
            "StudyInstanceUID", "AccessionNumber", "StudyDate", "StudyDescription", "StudyID", "ModalitiesInStudy", "NumberOfStudyRelatedSeries", "InstitutionName", "ManufacturerModelName", "Manufacturer"};
    String[] optVals_SeriesLevel = {"PatientID", "PatientName", "IssuerOfPatientID", "PatientBirthDate", "PatientSex",
            // Study Attributes
            "StudyInstanceUID", "AccessionNumber", "StudyDate", "StudyDescription", "StudyID", "ModalitiesInStudy", "InstitutionName",
            // Series Attr
            "SeriesInstanceUID", "SeriesDescription", "SeriesDate", "SeriesTime", "SeriesNumber", "Modality", "InstanceAvailability", "NumberOfSeriesRelatedInstances", "ManufacturerModelName", "Manufacturer"};
    String[] optVals_ImageLevel = {"PatientID", "PatientName", "IssuerOfPatientID", "PatientBirthDate", "PatientSex",
            // Study Attributes
            "StudyInstanceUID", "AccessionNumber", "StudyDate", "StudyDescription", "StudyID", "ModalitiesInStudy", "InstitutionName",
            // Series Attr
            "SeriesInstanceUID", "SeriesDescription", "SeriesDate", "SeriesTime", "SeriesNumber", "Modality", "InstanceAvailability", "NumberOfSeriesRelatedInstances", "ManufacturerModelName", "Manufacturer",
            // Image Attributes
            "SOPInstanceUID", "SOPClassUID", "NumberOfFrames", "InstanceNumber"};
    QueryFilter queryFilter = new QueryFilter(keys);
    private int priority;
    private int cancelAfter = 0;
    private InformationModel model;
    private DecimalFormat outFileFormat;
    private OutputStream out;
    private Association as;
    private IResultListener resultListener;

    public NifiFindScu(String callingAET, String calledAET, String host, int port, int queryLevel){
        device.addConnection(conn);
        applicationEntity = new ApplicationEntity(callingAET);
        device.addApplicationEntity(applicationEntity);
        applicationEntity.addConnection(conn);

        rq.setCalledAET(calledAET);
        remote.setHostname(host);
        remote.setPort(port);
        remote.setHttpProxy(null);

        configure(conn);
        remote.setTlsProtocols(conn.getTlsProtocols());
        remote.setTlsCipherSuites(conn.getTlsCipherSuites());
        setCancelAfter(0);
        setPriority(Priority.NORMAL);

        if (queryLevel == QUERY_LEVEL_PATIENT_STUDY){
            configureFindSCUForPatStudyLevel();
        }else if (queryLevel == QUERY_LEVEL_SERIES){
            configureFindSCUForSeriesLevel();
        }else if (queryLevel == QUERY_LEVEL_IMAGE){
            configureFindSCUForImageLevel();
        }else{
            configureFindSCUForPatStudyLevel();
        }
    }

    private static void configureKeys(NifiFindScu that, FIND_LEVEL level, String[] optVals) {

        addEmptyAttributes(that.getKeys(), optVals);
        that.addLevel(String.valueOf(level));
    }

    private static void addEmptyAttributes(Attributes attrs, String[] optVals) {
        if (optVals != null) {
            for (int i = 0; i < optVals.length; i++) {
                addAttributes(attrs, toTags(StringUtils.split(optVals[i], '.')));
            }
        }
    }

    private static int[] toTags(String[] tagOrKeywords) {
        int[] tags = new int[tagOrKeywords.length];
        for (int i = 0; i < tags.length; i++) {
            tags[i] = toTag(tagOrKeywords[i]);
        }
        return tags;
    }

    private static int toTag(String tagOrKeyword) {
        try {
            return Integer.parseInt(tagOrKeyword, 16);
        } catch (IllegalArgumentException e) {
            int tag = ElementDictionary.tagForKeyword(tagOrKeyword, null);
            if (tag == -1) {
                throw new IllegalArgumentException(tagOrKeyword);
            }
            return tag;
        }
    }

    private static void addAttributes(Attributes attrs, int[] tags, String... ss) {
        Attributes item = attrs;
        for (int i = 0; i < tags.length - 1; i++) {
            int tag = tags[i];
            Sequence sq = item.getSequence(tag);
            if (sq == null) {
                sq = item.newSequence(tag, 1);
            }
            if (sq.isEmpty()) {
                sq.add(new Attributes());
            }
            item = sq.get(0);
        }
        int tag = tags[tags.length - 1];
        VR vr = ElementDictionary.vrOf(tag, item.getPrivateCreator(tag));
        if (ss.length == 0 || ss.length == 1 && ss[0].isEmpty()) {
            if (vr == VR.SQ) {
                item.newSequence(tag, 1).add(new Attributes(0));
            } else {
                item.setNull(tag, vr);
            }
        } else {
            item.setString(tag, vr, ss);
        }
    }

    private static EnumSet<QueryOption> queryOptionsOf(boolean qo_relational, boolean qo_datetime, boolean qo_fuzzy, boolean qo_timezone) {
        EnumSet<QueryOption> queryOptions = EnumSet.noneOf(QueryOption.class);
        if (qo_relational) {
            queryOptions.add(QueryOption.RELATIONAL);
        }
        if (qo_datetime) {
            queryOptions.add(QueryOption.DATETIME);
        }
        if (qo_fuzzy) {
            queryOptions.add(QueryOption.FUZZY);
        }
        if (qo_timezone) {
            queryOptions.add(QueryOption.TIMEZONE);
        }
        return queryOptions;
    }

    private static String[] transferSyntaxesOf() {
        return IVR_LE_FIRST;
    }

    private static void configureServiceClass(NifiFindScu that, InformationModel model, boolean qo_relational, boolean qo_datetime, boolean qo_fuzzy, boolean qo_timezone) {
        that.setInformationModel(model, transferSyntaxesOf(), queryOptionsOf(qo_relational, qo_datetime, qo_fuzzy, qo_timezone));
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


    public QueryFilter getQueryFilter() {
        return this.queryFilter;
    }

    private void configureFindSCUForPatStudyLevel() {

        configureServiceClass(this, StudyRoot, false, false, false, false);

        configureKeys(this, FIND_LEVEL.STUDY, optVals_StudyLevel);

    }

    private void configureFindSCUForSeriesLevel() {
        /**
         * Use Relational Query
         * For Series Level
         */
        configureServiceClass(this, StudyRoot, true, false, false, false);
        configureKeys(this, FIND_LEVEL.SERIES, optVals_SeriesLevel);
    }

    private void configureFindSCUForImageLevel() {
        /**
         * Use Relational Query
         * For Series Level
         */
        configureServiceClass(this, StudyRoot, true, false, false, false);
        configureKeys(this, FIND_LEVEL.IMAGE, optVals_ImageLevel);
    }

    public final void setPriority(int priority) {
        this.priority = priority;
    }

    public final void setInformationModel(InformationModel model, String[] tss, EnumSet<QueryOption> queryOptions) {
        this.model = model;
        rq.addPresentationContext(new PresentationContext(1, model.cuid, tss));
        if (!queryOptions.isEmpty()) {
            model.adjustQueryOptions(queryOptions);
            rq.addExtendedNegotiation(new ExtendedNegotiation(model.cuid, QueryOption.toExtendedNegotiationInformation(queryOptions)));
        }
        if (model.level != null) {
            addLevel(model.level);
        }
    }

    public void addLevel(String s) {
        keys.setString(Tag.QueryRetrieveLevel, VR.CS, s);
    }

    public final void setCancelAfter(int cancelAfter) {
        this.cancelAfter = cancelAfter;
    }




    public Attributes getKeys() {
        return keys;
    }

    private void open() throws IOException, InterruptedException, IncompatibleConnectionException, GeneralSecurityException {
        as = applicationEntity.connect(conn, remote, rq);
    }

    public void close() throws IOException, InterruptedException {
        if (as != null && as.isReadyForDataTransfer()) {
            as.waitForOutstandingRSP();
            as.release();
        }
        SafeClose.close(out);
        out = null;
    }

    public void query() throws IOException, InterruptedException {
        query(keys);
    }

    private void query(Attributes keys) throws IOException, InterruptedException {
        DimseRSPHandler rspHandler = new DimseRSPHandler(as.nextMessageID()) {

            int numMatches;

            @Override
            public void onDimseRSP(Association as, Attributes cmd, Attributes data) {
                super.onDimseRSP(as, cmd, data);
                int status = cmd.getInt(Tag.Status, -1);
                if (Status.isPending(status)) {
                    if (null != NifiFindScu.this.resultListener) {
                        NifiFindScu.this.resultListener.onResult(data);
                    }
                    ++numMatches;
                    if (cancelAfter != 0 && numMatches >= cancelAfter) {
                        try {
                            cancel(as);
                            cancelAfter = 0;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        };

        query(keys, rspHandler);
    }


    private void query(Attributes keys, DimseRSPHandler rspHandler) throws IOException, InterruptedException {
        as.cfind(model.cuid, priority, keys, null, rspHandler);
    }

    public void doQuery(IConnectionSuccess success, IResultListener resultListener) throws Exception {
        this.resultListener = resultListener;
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        this.device.setExecutor(executorService);
        this.device.setScheduledExecutor(scheduledExecutorService);
        try {
            open();
            if (null != success){
                success.onConnected(this.remote);
            }
            query();
        } catch (Exception e) {
            throw new Exception("Could not open connection. " + e.getMessage());
        } finally {
            try {
                close();
                executorService.shutdown();
                executorService.awaitTermination(10, TimeUnit.SECONDS);
                scheduledExecutorService.shutdown();
                scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.info("Could not close connection or it is not opened!");
            }
        }
    }

    public enum InformationModel {
        PatientRoot(UID.PatientRootQueryRetrieveInformationModelFind, "STUDY"),
        StudyRoot(UID.StudyRootQueryRetrieveInformationModelFind, "STUDY"),
        PatientStudyOnly(UID.PatientStudyOnlyQueryRetrieveInformationModelFind, "STUDY"),
        MWL(UID.ModalityWorklistInformationModelFind, null), UPSPull(UID.UnifiedProcedureStepPull, null),
        UPSWatch(UID.UnifiedProcedureStepWatch, null), UPSQuery(UID.UnifiedProcedureStepQuery, null),
        HangingProtocol(UID.HangingProtocolInformationModelFind, null),
        ColorPalette(UID.ColorPaletteQueryRetrieveInformationModelFind, null);

        final String cuid;
        final String level;

        InformationModel(String cuid, String level) {
            this.cuid = cuid;
            this.level = level;
        }

        public void adjustQueryOptions(EnumSet<QueryOption> queryOptions) {
            if (level == null) {
                queryOptions.add(QueryOption.RELATIONAL);
                queryOptions.add(QueryOption.DATETIME);
            }
        }
    }

}
