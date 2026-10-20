package org.rra.cget;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.net.Association;
import org.dcm4che3.net.PDVInputStream;
import org.dcm4che3.net.Status;
import org.dcm4che3.net.pdu.PresentationContext;
import org.dcm4che3.net.service.BasicCStoreSCP;
import org.dcm4che3.net.service.DicomServiceException;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class NifiCGetCSotreSCP extends BasicCStoreSCP {

    private final ProcessSessionFactory sessionFactory;
    private final Relationship relationship;
    private final Map<String, String> orderAttributes;

    public NifiCGetCSotreSCP(
            ProcessSessionFactory sessionFactory,
            Relationship relationship,
            Map<String, String> orderAttributes) {

        super("*");
        this.sessionFactory = sessionFactory;
        this.relationship = relationship;
        this.orderAttributes = Map.copyOf(orderAttributes);
    }

    @Override
    protected void store(
            Association as,
            PresentationContext pc,
            Attributes rq,
            PDVInputStream data,
            Attributes rsp) throws DicomServiceException {

        final long startNanos = System.nanoTime();

        final String cuid = rq.getString(Tag.AffectedSOPClassUID);
        final String iuid = rq.getString(Tag.AffectedSOPInstanceUID);
        final String tsuid = pc.getTransferSyntax();

        ProcessSession session = null;
        boolean committed = false;

        try {
            session = sessionFactory.createSession();

            FlowFile flowFile = session.create();

            if (!orderAttributes.isEmpty()) {
                flowFile = session.putAllAttributes(
                        flowFile, orderAttributes);
            }

            // C-STORE-Datensatz schreiben.
            // Wie bisher ohne Part-10 File Meta Information.
            try (OutputStream out = session.write(flowFile);
                 BufferedOutputStream bos =
                         new BufferedOutputStream(out)) {
                data.copyTo(bos);
            }

            // Erst nach Schliessen des Schreibstreams lesen.
            final Attributes dicomAttributes;
            try (InputStream in = session.read(flowFile);
                 DicomInputStream din = new DicomInputStream(in)) {

                din.setIncludeBulkData(
                        DicomInputStream.IncludeBulkData.NO);

                dicomAttributes = din.readDatasetUntilPixelData();
            }

            final String studyUID =
                    dicomAttributes.getString(Tag.StudyInstanceUID);
            final String seriesUID =
                    dicomAttributes.getString(Tag.SeriesInstanceUID);

            if (studyUID == null || seriesUID == null) {
                throw new IOException(
                        "Missing StudyInstanceUID or SeriesInstanceUID");
            }

            final String callingAET = as.getCallingAET();
            final String calledAET = as.getCalledAET();

            Map<String, String> attrs = new HashMap<>();
            attrs.put("AffectedSOPClassUID", cuid);
            attrs.put("AffectedSOPInstanceUID", iuid);
            attrs.put("TransferSyntax", tsuid);
            attrs.put("CallingAET", callingAET);
            attrs.put("CalledAET", calledAET);
            attrs.put("StudyInstanceUID", studyUID);
            attrs.put("SeriesInstanceUID", seriesUID);
            attrs.put("PatientID",
                    dicomAttributes.getString(Tag.PatientID, "NO-ID"));
            attrs.put("Modality",
                    dicomAttributes.getString(Tag.Modality, "OT"));

            MessageDigest md5 = MessageDigest.getInstance("MD5");

            attrs.put("HexStudyIUID",
                    HexFormat.of().formatHex(md5.digest(
                            studyUID.getBytes(StandardCharsets.UTF_8))));

            attrs.put("HexSeriesIUID",
                    HexFormat.of().formatHex(md5.digest(
                            seriesUID.getBytes(StandardCharsets.UTF_8))));

            flowFile = session.putAllAttributes(flowFile, attrs);

            flowFile = session.putAttribute(
                    flowFile,
                    CoreAttributes.FILENAME.key(),
                    iuid + ".dcm");

            flowFile = session.putAttribute(
                    flowFile,
                    CoreAttributes.MIME_TYPE.key(),
                    "application/dicom");

            long durationMillis = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - startNanos);

            session.getProvenanceReporter().receive(
                    flowFile, callingAET, durationMillis);

            session.transfer(flowFile, relationship);
            session.commit();
            committed = true;

            rsp.setInt(Tag.Status, VR.US, Status.Success);

            log.info(
                    "C-GET C-STORE success: SOPInstanceUID={}, " +
                            "StudyInstanceUID={}, SeriesInstanceUID={}, " +
                            "duration={}ms",
                    iuid, studyUID, seriesUID, durationMillis);

        } catch (Exception e) {

            log.error(
                    "C-GET C-STORE failed: SOPInstanceUID={}",
                    iuid, e);

            if (session != null && !committed) {
                try {
                    session.rollback();
                } catch (Exception rollbackException) {
                    log.error(
                            "Session rollback failed",
                            rollbackException);
                }
            }

            throw new DicomServiceException(
                    Status.ProcessingFailure, e);
        }
    }
}