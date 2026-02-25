package org.rra.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.dcm4che3.data.UID;
import org.rra.dcm.Dicom2DicomTranscoder;

import java.io.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * jpeg=compress JPEG Lossy; equivalent to -t 1.2.840.10008.1.2.4.50 or .51
 * jpll=compress JPEG Lossless; equivalent to -t 1.2.840.10008.1.2.4.70
 * jlsl=compress JPEG LS Lossless; equivalent to -t 1.2.840.10008.1.2.4.80
 * jlsn=compress JPEG LS Lossy (Near-Lossless); equivalent to -t 1.2.840.10008.1.2.4.81
 * j2kr=compress JPEG 2000 Lossless; equivalent to -t 1.2.840.10008.1.2.4.90
 * j2ki=compress JPEG 2000 Lossy; equivalent to -t 1.2.840.10008.1.2.4.91
 * defl=transcode sources to Deflated Explicit VR Little Endian; equivalent to
 * default=transcode ImplicitVRLittleEndian -t 1.2.840.10008.1.2
 * <p>
 * -N <near-lossless>           Near-Lossless parameter of JPEG LS Lossy
 * compression
 * -q <quality>                 compression quality (0.0-1.0) of JPEG Lossy
 * compression
 * -Q <compression>             compression factor (5-100) of JPEG 2000
 * Lossy compression
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@SystemResourceConsideration(resource = SystemResource.CPU)

@Tags({"DICOM", "Dcm2Dcm", "CDP"})
@CapabilityDescription("Transcode one DICOM Object according the specified Transfer Syntax")
@UseCase(description = "Compress or Uncompress DICOM Objects.",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)

public class Dcm2Dcm extends AbstractProcessor {
    public static final String JPEG = "JPEG Lossy";
    public static final String JPLL = "JPEG Lossless";
    public static final String JPLSL = "JPEG LS Lossless";
    public static final String JPLSN = "JPEG LS Lossy (Near-Lossless)";
    public static final String JP2KR = "JPEG 2000 Lossless";
    public static final String JP2KI = "JPEG 2000 Lossy";
    public static final String DEFL = "Deflated Explicit VR Little Endian";
    public static final String IVRLE = "Implicit VR Little Endian (Default TS)";
    public static final String EVRLE = "Explicit VR Little Endian";

    public static final PropertyDescriptor TRANSFER_SYNTAX = new PropertyDescriptor.Builder()
            .name("transfer-syntax")
            .displayName("Transfer Syntax")
            .description("The Transfer Syntax")
            .required(true)
            .allowableValues(IVRLE, EVRLE, JPEG, JPLL, JPLSL, JPLSN, JP2KR, JP2KI, DEFL)
            .defaultValue(IVRLE)
            .addValidator(Validator.VALID)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Sending success relationship of the SCU")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to send DICOM Data.").build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private static String transferSyntaxOf(String ts) {
        return ts.equals(IVRLE) ? UID.ImplicitVRLittleEndian
                : ts.equals(EVRLE) ? UID.ExplicitVRLittleEndian
                : ts.equals(DEFL) ? UID.DeflatedExplicitVRLittleEndian
                : ts.equals(JPEG) ? UID.JPEGBaseline8Bit
                : ts.equals(JPLL) ? UID.JPEGLosslessSV1
                : ts.equals(JPLSL) ? UID.JPEGLSLossless
                : ts.equals(JPLSN) ? UID.JPEGLSNearLossless
                : ts.equals(JP2KR) ? UID.JPEG2000Lossless
                : ts.equals(JP2KI) ? UID.JPEG2000
                : UID.ImplicitVRLittleEndian;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(TRANSFER_SYNTAX);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @OnScheduled
    protected void start(final ProcessContext context) {
        final ComponentLog log = getLogger();
        log.info("+ + + Start {} OK. + + +", getClass().getSimpleName());
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final ComponentLog log = getLogger();
        final String ts_orig = flowFile.getAttribute("TransferSyntax");
        final String ts_option = context.getProperty(TRANSFER_SYNTAX).getValue();
        final String transferSyntax = transferSyntaxOf(ts_option);
        try {
            final long t1 = System.nanoTime();
            flowFile = session.write(flowFile, (in, out) -> {
                try (OutputStream buffOut = new BufferedOutputStream(out)) {
                    try (InputStream buffIn = new BufferedInputStream(in)) {
                        Dicom2DicomTranscoder.transcode(buffIn, buffOut, transferSyntax);
                    } catch (Exception exception) {
                        throw new IOException(exception);
                    }
                } catch (Exception exception) {
                    throw exception;
                }
            });
            flowFile = session.putAttribute(flowFile, "TransferSyntax", transferSyntax);
            final long importNanos = System.nanoTime() - t1;
            final long transcodeMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);
            session.getProvenanceReporter().modifyContent(flowFile, transcodeMillis);
            session.transfer(flowFile, REL_SUCCESS);
            log.info("$ $ $ Transcode from [{}] to [{} - {}] $ $ $", ts_orig, transferSyntax, ts_option);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

}
