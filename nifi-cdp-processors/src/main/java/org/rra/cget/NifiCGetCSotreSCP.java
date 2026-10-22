package org.rra.cget;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;
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

        final String cuid =
                rq.getString(Tag.AffectedSOPClassUID);

        final String iuid =
                rq.getString(Tag.AffectedSOPInstanceUID);

        final String tsuid =
                pc.getTransferSyntax();

        ProcessSession session = null;
        boolean committed = false;

        try {
            session = sessionFactory.createSession();

            FlowFile flowFile = session.create();

            // Order-Attribute übernehmen
            if (!orderAttributes.isEmpty()) {
                flowFile = session.putAllAttributes(
                        flowFile, orderAttributes);
            }

            // Genau wie dcm4che:
            // File Meta Information + empfangenes Dataset
            try (OutputStream out = session.write(flowFile);
                 BufferedOutputStream bos =
                         new BufferedOutputStream(out);
                 DicomOutputStream dos =
                         new DicomOutputStream(
                                 bos, UID.ExplicitVRLittleEndian)) {

                dos.writeFileMetaInformation(
                        as.createFileMetaInformation(
                                iuid, cuid, tsuid));

                data.copyTo(dos);
            }

            Map<String, String> attrs = new HashMap<>();

            attrs.put("AffectedSOPClassUID", cuid);
            attrs.put("AffectedSOPInstanceUID", iuid);
            attrs.put("TransferSyntax", tsuid);
            attrs.put("CallingAET", as.getCallingAET());
            attrs.put("CalledAET", as.getCalledAET());

            flowFile = session.putAllAttributes(
                    flowFile, attrs);

            flowFile = session.putAttribute(
                    flowFile,
                    CoreAttributes.FILENAME.key(),
                    iuid + ".dcm");

            flowFile = session.putAttribute(
                    flowFile,
                    CoreAttributes.MIME_TYPE.key(),
                    "application/dicom");

            session.transfer(flowFile, relationship);

            // Erst nach erfolgreichem Commit C-STORE bestätigen
            session.commit();
            committed = true;

            rsp.setInt(
                    Tag.Status,
                    VR.US,
                    Status.Success);

            log.info(
                    "C-STORE success: SOPInstanceUID={}, TransferSyntax={}",
                    iuid, tsuid);

        } catch (Exception e) {

            log.error(
                    "C-STORE failed: SOPInstanceUID={}",
                    iuid, e);

            if (session != null && !committed) {
                try {
                    session.rollback();
                } catch (Exception rollbackException) {
                    log.error(
                            "C-STORE rollback failed",
                            rollbackException);
                }
            }

            throw new DicomServiceException(
                    Status.ProcessingFailure, e);
        }
    }
}