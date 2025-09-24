package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.util.TagUtils;
import org.rra.cmove.NifiMoveScu;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"CDP", "DICOM", "Move-SCU"})
@CapabilityDescription("Make a C-move Request to remote host to move Study or Series to the move destination.")
@UseCase(description = "DICOM C-Move to move data from remote archives. Can be used for DICOM Query/Retrieve process.",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)

public class MoveScu extends AbstractProcessor {
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
    public static final PropertyDescriptor MOVE_AET = new PropertyDescriptor.Builder()
            .name("move-AET")
            .displayName("Move AET")
            .description("The AE Title of this remote destination")
            .defaultValue("DCM4NIFI")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    public static final PropertyDescriptor MOVE_LEVEL = new PropertyDescriptor.Builder()
            .name("move-level")
            .displayName("Move Level")
            .description("The Level of C-Move")
            .required(true)
            .allowableValues(STUDY_LEVEL, SERIES_LEVEL)
            .defaultValue(STUDY_LEVEL)
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship of the DICOM C-Move process")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("DICOM C-Move Failed").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(REMOTE_HOST, PORT, CALLED_AET, CALLING_AET, MOVE_AET, MOVE_LEVEL);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
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
        final ComponentLog log = getLogger();
        String called_aet = context.getProperty(CALLED_AET).evaluateAttributeExpressions().getValue();
        String calling_aet = context.getProperty(CALLING_AET).evaluateAttributeExpressions().getValue();
        String move_aet = context.getProperty(MOVE_AET).evaluateAttributeExpressions().getValue();
        String remoteHost = context.getProperty(REMOTE_HOST).evaluateAttributeExpressions().getValue();
        int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        String level = context.getProperty(MOVE_LEVEL).evaluateAttributeExpressions().getValue();
        log.info("move AET: " + move_aet);
        try{
            final long t1 = System.nanoTime();
            try(InputStream read = session.read(flowFile)) {
                Attributes request = readDicomObject(read);
                NifiMoveScu nifiMoveSCU = new NifiMoveScu(remoteHost, port, calling_aet, called_aet, move_aet);
                log.info("Level {}", level);
                if (level.equals(STUDY_LEVEL)) {
                    String studyIUID = readStudyIUID(request);
                    nifiMoveSCU.moveStudy(studyIUID );
                } else if  (level.equals(SERIES_LEVEL)) {
                    String studyIUID = readStudyIUID(request);
                    String seriesIUID = readSeriesIUID(request);
                    nifiMoveSCU.moveSeries(studyIUID, seriesIUID);
                }
            }
            final long importNanos = System.nanoTime() - t1;
            final long importMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);
            final String details = "Destination_AET:"+move_aet+" Status:"+ TagUtils.shortToHexString(0);
            log.info(details);
            session.getProvenanceReporter().route(flowFile, REL_SUCCESS, details, importMillis);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            session.getProvenanceReporter().route(flowFile, REL_FAILURE, e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private static Attributes readDicomObject(InputStream in) throws IOException {
        DicomInputStream din = new DicomInputStream(in);
        return din.readDatasetUntilPixelData();
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
        final ComponentLog log = getLogger();
        log.info("+ + + Start {} OK. + + +", getClass().getSimpleName());
    }

}
