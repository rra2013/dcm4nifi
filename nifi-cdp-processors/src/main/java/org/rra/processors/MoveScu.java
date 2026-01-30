package org.rra.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.util.TagUtils;
import org.rra.cmove.IMoveComplete;
import org.rra.cmove.IMoveHasErrors;
import org.rra.cmove.NifiMoveScu;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
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
            //.expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("remote-port")
            .displayName("Remote Port")
            .description("The Server Port of DICOM Move-SCP")
            .required(true)
            .defaultValue("11112")
            //.expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor CALLED_AET = new PropertyDescriptor.Builder()
            .name("called-AET")
            .displayName("Called AET")
            .description("The AE Title of the remote SCP")
            .defaultValue("DCM4CHEE")
            .required(true)
            //.expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor CALLING_AET = new PropertyDescriptor.Builder()
            .name("calling-AET")
            .displayName("Calling AET")
            .description("The AE Title of this SCU")
            .defaultValue("NIFI_SCU")
            .required(true)
            //.expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    public static final PropertyDescriptor MOVE_AET = new PropertyDescriptor.Builder()
            .name("move-AET")
            .displayName("Move AET")
            .description("The AE Title of this remote destination")
            .defaultValue("DCM4NIFI")
            .required(true)
            //.expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
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
        String called_aet = context.getProperty(CALLED_AET).getValue();
        String calling_aet = context.getProperty(CALLING_AET).getValue();
        String move_aet = context.getProperty(MOVE_AET).getValue();
        String remoteHost = context.getProperty(REMOTE_HOST).getValue();
        int port = context.getProperty(PORT).asInteger();
        String level = context.getProperty(MOVE_LEVEL).getValue();
        log.info("move AET: " + move_aet);
        try {
            final long t1 = System.nanoTime();
            final CountDownLatch done = new CountDownLatch(1);
            final AtomicReference<Relationship> rel = new AtomicReference<>(REL_FAILURE);
            final AtomicReference<String> detailsRef = new AtomicReference<>("unknown");
            final AtomicBoolean finalized = new AtomicBoolean(false);

            Attributes request;
            try (InputStream read = session.read(flowFile)) {
                request = readDicomObject(read);
            }

            NifiMoveScu nifiMoveSCU = new NifiMoveScu(remoteHost, port, calling_aet, called_aet, move_aet);
            log.info("Level {}", level);
            IMoveComplete completeHandler = (studyIUID, seriesIUID) -> {
                if (!finalized.compareAndSet(false, true)) return;
                rel.set(REL_SUCCESS);
                detailsRef.set("Destination_AET:" + move_aet
                        + " Status:" + TagUtils.shortToHexString(0)
                        + " tookMs=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1));
                done.countDown();
                log.info("Moved -> Study UID {}, Series UID {}", studyIUID, seriesIUID);
            };
            IMoveHasErrors hasErrorHandler = (status, message) -> {
                if (!finalized.compareAndSet(false, true)) return;
                rel.set(REL_FAILURE);
                detailsRef.set(message != null ? message : ("C-MOVE failed status=" + status));
                done.countDown();
                log.info("Move has error:{} - {}", status, message);
            };
            // Move Study/Series
            if (level.equals(STUDY_LEVEL)) {
                String studyIUID = readStudyIUID(request);
                nifiMoveSCU.moveStudy(studyIUID, completeHandler, hasErrorHandler);
            } else if (level.equals(SERIES_LEVEL)) {
                String studyIUID = readStudyIUID(request);
                String seriesIUID = readSeriesIUID(request);
                nifiMoveSCU.moveSeries(studyIUID, seriesIUID, completeHandler, hasErrorHandler);
            } else {
                throw new IllegalArgumentException("Unsupported move level: " + level);
            }

            // Warten auf final (z.B. 5 Min)
            boolean finished = done.await(5, TimeUnit.MINUTES);
            log.info("Move SCU: finished={} rel={} details={}", finished, rel.get().getName(), detailsRef.get());

            if (!finished && finalized.compareAndSet(false, true)) {
                detailsRef.set("C-MOVE timed out (no final response)");
                rel.set(REL_FAILURE);
            }

            Relationship r = rel.get();
            String details = detailsRef.get();

            if (r == REL_SUCCESS) {
                flowFile = session.putAttribute(flowFile, "cmove.details", details);
            } else {
                flowFile = session.putAttribute(flowFile, "cmove.error", details);
            }
            session.transfer(flowFile, r);
        } catch (Exception e) {
            log.error("Failed to move AET", e);
            String msg = (e.getMessage() != null) ? e.getMessage() : e.toString();
            flowFile = session.putAttribute(flowFile, "cmove.error", msg);
            session.transfer(flowFile, REL_FAILURE);
        }

    }

    @OnScheduled
    protected void start(final ProcessContext context) {
        final ComponentLog log = getLogger();
        log.info("+ + + Start {} OK. + + +", getClass().getSimpleName());
    }

}
