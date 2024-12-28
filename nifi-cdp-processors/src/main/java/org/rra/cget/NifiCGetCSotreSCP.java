package org.rra.cget;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.net.Association;
import org.dcm4che3.net.PDVInputStream;
import org.dcm4che3.net.Status;
import org.dcm4che3.net.pdu.PresentationContext;
import org.dcm4che3.net.service.BasicCStoreSCP;
import org.dcm4che3.net.service.DicomServiceException;
import org.dcm4che3.util.SafeClose;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class NifiCGetCSotreSCP extends BasicCStoreSCP {
    private final ProcessSession session;
    private final Relationship relationship;
    private int ctr = 0;
    public NifiCGetCSotreSCP(ProcessSession session, Relationship relationship) {
        super("*");
        this.session = session;
        this.relationship = relationship;

    }

    @Override
    protected void store(Association as, PresentationContext pc, Attributes rq,
                         PDVInputStream data, Attributes rsp)
            throws ProcessException, DicomServiceException {
        ctr += 1;
        log.info("$ $ $ $ $ $ {} $ $ $ $ $", ctr);

        String cuid = rq.getString(Tag.AffectedSOPClassUID);
        String iuid = rq.getString(Tag.AffectedSOPInstanceUID);
        String tsuid = pc.getTransferSyntax();

        FlowFile flowFile = session.create();
        try {
            long t1 = System.nanoTime();
           try (OutputStream flowFileOutputStream = session.write(flowFile)) {
                try(BufferedOutputStream bos = new BufferedOutputStream(flowFileOutputStream)){
                    storeOnlyAttributesTo(bos, data);
                }
                log.info("+ + + DICOM Object received -> SOPIUID: {} + + +", iuid);
            } catch (SocketException socketException) {
                log.error("Socket exception during data transfer", socketException);
                session.rollback();
                throw new IOException(socketException.getMessage());
            } catch (IOException ioException) {
                log.error("IOException during data transfer", ioException);
                session.rollback();
                throw new IOException(ioException.getMessage());
            }
            try {
                final String callingAET = as.getAAssociateAC().getCallingAET();
                final String calledAET = as.getAAssociateAC().getCalledAET();
                session.putAttribute(flowFile, "AffectedSOPClassUID", cuid);
                session.putAttribute(flowFile, "AffectedSOPInstanceUID", iuid);
                session.putAttribute(flowFile, "TransferSyntax", tsuid);
                session.putAttribute(flowFile, "CallingAET", callingAET);
                session.putAttribute(flowFile, "CalledAET", calledAET);
                //Transfer application/dicom
                flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/dicom");
                final long importNanos = System.nanoTime() - t1;
                final long importMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);
                session.getProvenanceReporter().receive(flowFile, callingAET, importMillis);
                //
                session.transfer(flowFile, relationship);
            } catch (Exception exception) {
                session.rollback();
                log.error("Process session error. ", exception);
            }
            session.commitAsync(() -> {
                // if data transfer ok - send transfer complete message
                log.info("# # # Process Complete # # #");
            });

        } catch (Exception e) {
            throw new DicomServiceException(Status.ProcessingFailure, e);
        }

    }

    private void storeOnlyAttributesTo(OutputStream outputStream, PDVInputStream data){
        try {
            DicomOutputStream out = new DicomOutputStream(outputStream, UID.ExplicitVRLittleEndian);
            try {
                data.copyTo(out);
            } finally {
                SafeClose.close(out);
            }
        } catch (Exception e) {
        }
    }

}
