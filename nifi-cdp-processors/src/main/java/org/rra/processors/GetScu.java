package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.util.TagUtils;
import org.rra.cget.NifiGetScu;
import org.rra.dcm.DicomUtils;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"CDP", "DICOM", "Get-SCU"})
@CapabilityDescription("Make a C-Get Request to remote host to retrieve Study or Series.")
@UseCase(description = "DICOM C-Get to retrieve data from remote archives. Can be used for DICOM Query/Retrieve process.",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)

public class GetScu extends AbstractProcessor {
    public static final String STUDY_LEVEL = "Study";
    public static final String SERIES_LEVEL = "Series";
    public static final PropertyDescriptor REMOTE_HOST = new PropertyDescriptor.Builder()
            .name("remote-address")
            .displayName("Remote Address")
            .description("The address of the Remote Move-SCP server.")
            .required(true)
            .defaultValue("")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("remote-port")
            .displayName("Remote Port")
            .description("The Server Port of DICOM Move-SCP")
            .required(true)
            .defaultValue("11112")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor CALLED_AET = new PropertyDescriptor.Builder()
            .name("called-AET")
            .displayName("Called AET")
            .description("The AE Title of the remote SCP")
            .defaultValue("DCM4CHEE")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor CALLING_AET = new PropertyDescriptor.Builder()
            .name("calling-AET")
            .displayName("Calling AET")
            .description("The AE Title of this SCU")
            .defaultValue("NIFI_SCU")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor GET_LEVEL = new PropertyDescriptor.Builder()
            .name("get-level")
            .displayName("Get Level")
            .description("The Level of C-Get")
            .required(true)
            .allowableValues(STUDY_LEVEL, SERIES_LEVEL)
            .defaultValue(STUDY_LEVEL)
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship of the DICOM C-Move process")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The relationship of the DICOM C-Get original input flow file")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("DICOM C-Move Failed").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(REMOTE_HOST, PORT, CALLED_AET, CALLING_AET, GET_LEVEL);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE, REL_ORIGINAL);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        String called_aet = context.getProperty(CALLED_AET).evaluateAttributeExpressions().getValue();
        String calling_aet = context.getProperty(CALLING_AET).evaluateAttributeExpressions().getValue();
        String remoteHost = context.getProperty(REMOTE_HOST).evaluateAttributeExpressions().getValue();
        int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        String level = context.getProperty(GET_LEVEL).evaluateAttributeExpressions().getValue();
        AtomicBoolean hasError = new AtomicBoolean(false);

        Attributes request = null;
        try (InputStream read = session.read(flowFile)) {
            request = DicomUtils.readDicomObjectUntilPixelData(read);
        } catch (IOException e) {
            session.getProvenanceReporter().route(flowFile, REL_FAILURE);
            session.transfer(flowFile, REL_FAILURE);
        }

        try {
            final long t1 = System.nanoTime();
            session.remove(flowFile);
            session.commitAsync();
            NifiGetScu nifiGetScu = new NifiGetScu(remoteHost, port, calling_aet, called_aet, session, REL_SUCCESS);
            log.info("Level {}", level);
            if (level.equals(STUDY_LEVEL)) {
                String studyIUID = readStudyIUID(request);
                nifiGetScu.getOnStudyLevel(studyIUID);
            } else if (level.equals(SERIES_LEVEL)) {
                String studyIUID = readStudyIUID(request);
                String seriesIUID = readSeriesIUID(request);
                nifiGetScu.getOnSeriesLevel(studyIUID, seriesIUID);
            }

            final long importNanos = System.nanoTime() - t1;
            final long importMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);
            final String details = "Retrieve_AET:" + called_aet + " Status:" + TagUtils.shortToHexString(0);
            log.info(details);
            FlowFile newFlowfile = session.create();
            try(OutputStream out = session.write(newFlowfile)){
                try(BufferedOutputStream bos = new BufferedOutputStream(out)){
                    if (null != request){
                        DicomUtils.copyAttributesToOutput(request, bos);
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/dicom");
            session.transfer(newFlowfile, REL_ORIGINAL);
            session.getProvenanceReporter().route(newFlowfile, REL_ORIGINAL, details, importMillis);

            session.commitAsync(() -> {
                log.info("Transfer Complete");
            });
        } catch (Exception e) {
            hasError.set(true);
        }
        if (hasError.get()) {
            FlowFile newFlowfile = session.create();
            try(OutputStream out = session.write(newFlowfile)){
                try(BufferedOutputStream bos = new BufferedOutputStream(out)){
                    if (null != request){
                        DicomUtils.copyAttributesToOutput(request, bos);
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            session.putAttribute(newFlowfile, CoreAttributes.MIME_TYPE.key(), "application/dicom");
            session.getProvenanceReporter().route(newFlowfile, REL_FAILURE);
            session.transfer(newFlowfile, REL_FAILURE);
            session.commitAsync(() -> {
                log.info("Transfer Complete");
            });
        }
    }



    private static String readStudyIUID(Attributes data) throws Exception {
        String uid = data.getString(Tag.StudyInstanceUID, null);
        if (null == uid) {
            throw new Exception("StudyInstanceUID is null");
        }
        return uid;
    }

    private static String readSeriesIUID(Attributes data) throws Exception {
        String uid = data.getString(Tag.SeriesInstanceUID, null);
        if (null == uid) {
            throw new Exception("SeriesInstanceUID is null");
        }
        return uid;
    }

    @OnScheduled
    protected void start(final ProcessContext context) {
        log.info("+ + + Start {} OK. + + +", getClass().getSimpleName());
    }
    @OnStopped
    public void stop(){
        log.info("+ + + Stop {} OK. + + +", getClass().getSimpleName());
    }
}
