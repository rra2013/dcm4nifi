package org.rra.dcm;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.rra.dcm.MaskMultiFrameUSTransformer;
import org.rra.dcm.MaskMultiFrameUSTransformer.LossyOutput;
import org.rra.dcm.MaskRegion;
import org.rra.dcm.RegionMaskParser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@SystemResourceConsideration(resource = SystemResource.CPU)
@SystemResourceConsideration(resource = SystemResource.DISK)
@Tags({"DICOM", "Ultrasound", "Multi-frame", "MaskPixel", "CDP"})
@CapabilityDescription("Masks configured pixel regions in every frame of Ultrasound Multi-frame Image Storage "
        + "instances. Inputs are decoded to Explicit VR Little Endian for masking. Losslessly encoded inputs are "
        + "re-encoded with their original transfer syntax. Lossy inputs (JPEG Baseline/Extended, JPEG-LS "
        + "Near-Lossless, JPEG 2000) are written according to 'Output for Lossy Input', by default RLE Lossless, "
        + "to avoid a second generation loss. Lossy Image Compression (0028,2110) stays '01' and Ratio/Method "
        + "(0028,2112/2114) are preserved, completed and, on lossy re-encoding, extended (PS3.3 C.7.6.1.1.5). "
        + "For YBR_FULL pixel data the RGB mask value is converted to YBR automatically. RLE Lossless is "
        + "written by a built-in encoder (PS3.5 Annex G) because dcm4che only provides an RLE decoder.")
@UseCase(description = "Masks burned-in annotations at the same configured image coordinates in every ultrasound frame.",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = MaskMultiFrameUS.ATTR_SOURCE_TS,
                description = "Transfer Syntax UID of the input instance"),
        @WritesAttribute(attribute = MaskMultiFrameUS.ATTR_OUTPUT_TS,
                description = "Transfer Syntax UID of the masked output instance"),
        @WritesAttribute(attribute = MaskMultiFrameUS.ATTR_ERROR,
                description = "Error message when the instance was routed to failure")
})
public class MaskMultiFrameUS extends AbstractProcessor {

    static final String ATTR_SOURCE_TS = "maskmultiframeus.source.transfer.syntax";
    static final String ATTR_OUTPUT_TS = "maskmultiframeus.output.transfer.syntax";
    static final String ATTR_ERROR = "maskmultiframeus.error";

    private static final Validator REGIONS_VALIDATOR = (subject, input, context) -> {
        try {
            RegionMaskParser.parse(input);
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        } catch (Exception e) {
            return new ValidationResult.Builder().subject(subject).input(input)
                    .valid(false).explanation(e.getMessage()).build();
        }
    };

    private static final Validator COLOR_VALIDATOR = (subject, input, context) -> {
        if (context.isExpressionLanguageSupported(subject)
                && context.isExpressionLanguagePresent(input)) {
            return new ValidationResult.Builder().subject(subject).input(input)
                    .valid(true).explanation("Expression Language wird zur Laufzeit ausgewertet").build();
        }
        try {
            parseColor(input);
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        } catch (IllegalArgumentException e) {
            return new ValidationResult.Builder().subject(subject).input(input)
                    .valid(false).explanation(e.getMessage()).build();
        }
    };

    static final AllowableValue LOSSY_OUTPUT_RLE = new AllowableValue(
            LossyOutput.RLE_LOSSLESS.name(), "RLE Lossless",
            "Verlustfrei (1.2.840.10008.1.2.5). Breit unterstützt, mässige Kompression.");
    static final AllowableValue LOSSY_OUTPUT_JPEG_LS = new AllowableValue(
            LossyOutput.JPEG_LS_LOSSLESS.name(), "JPEG-LS Lossless",
            "Verlustfrei (1.2.840.10008.1.2.4.80). Beste Kompression; benötigt den dcm4che-opencv-Writer "
                    + "inklusive nativer Bibliotheken auf dem NiFi-Node.");
    static final AllowableValue LOSSY_OUTPUT_EVRLE = new AllowableValue(
            LossyOutput.EXPLICIT_VR_LITTLE_ENDIAN.name(), "Explicit VR Little Endian",
            "Unkomprimiert (1.2.840.10008.1.2.1). Maximal kompatibel, aber gross.");
    static final AllowableValue LOSSY_OUTPUT_ORIGINAL = new AllowableValue(
            LossyOutput.ORIGINAL.name(), "Original (re-encode lossy)",
            "Original-Transfer-Syntax beibehalten. Erzeugt einen zweiten Generationsverlust; "
                    + "der neue Schritt wird in Ratio/Method angehängt.");

    public static final PropertyDescriptor REGIONS = new PropertyDescriptor.Builder()
            .name("regions")
            .displayName("Regions")
            .description("Mask regions in [x,y,width,height] format, e.g. [10,20,-1,50]. "
                    + "A negative width masks the full frame width. The same regions are applied to every frame.")
            .required(true)
            .addValidator(REGIONS_VALIDATOR)
            .build();

    public static final PropertyDescriptor COLOR = new PropertyDescriptor.Builder()
            .name("color")
            .displayName("Pixel Value")
            .description("Pixel value used to fill the configured mask regions; accepts decimal, 0x-prefixed, "
                    + "or #-prefixed values. For color images the value is interpreted as RGB (0xRRGGBB) and "
                    + "converted automatically when the decoded pixel data is YBR_FULL.")
            .required(true)
            .defaultValue("0")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(COLOR_VALIDATOR)
            .build();

    public static final PropertyDescriptor LOSSY_OUTPUT = new PropertyDescriptor.Builder()
            .name("lossy-output")
            .displayName("Output for Lossy Input")
            .description("Transfer syntax written for inputs that were lossy compressed. Lossless inputs always "
                    + "keep their original transfer syntax.")
            .required(true)
            .allowableValues(LOSSY_OUTPUT_RLE, LOSSY_OUTPUT_JPEG_LS, LOSSY_OUTPUT_EVRLE, LOSSY_OUTPUT_ORIGINAL)
            .defaultValue(LOSSY_OUTPUT_RLE.getValue())
            .build();

    public static final PropertyDescriptor MARK_CLEAN_PIXEL_DATA = new PropertyDescriptor.Builder()
            .name("mark-clean-pixel-data")
            .displayName("Mark Clean Pixel Data")
            .description("If true, sets Burned In Annotation (0028,0301) to NO and adds code 113101 "
                    + "'Clean Pixel Data Option' to the De-identification Method Code Sequence (0012,0064). "
                    + "Only enable this if the configured regions cover all burned-in identifying information.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Validated and masked Ultrasound Multi-frame DICOM instance")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Unsupported or failed DICOM instance; original content is routed here")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private volatile List<MaskRegion> regions;
    private volatile LossyOutput lossyOutput;
    private volatile boolean markCleanPixelData;

    @Override
    protected void init(ProcessorInitializationContext context) {
        descriptors = List.of(REGIONS, COLOR, LOSSY_OUTPUT, MARK_CLEAN_PIXEL_DATA);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        regions = RegionMaskParser.parse(context.getProperty(REGIONS).getValue());
        lossyOutput = LossyOutput.valueOf(context.getProperty(LOSSY_OUTPUT).getValue());
        markCleanPixelData = context.getProperty(MARK_CLEAN_PIXEL_DATA).asBoolean();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            int pixelValue = parseColor(context.getProperty(COLOR)
                    .evaluateAttributeExpressions(flowFile).getValue());
            MaskMultiFrameUSTransformer transformer = new MaskMultiFrameUSTransformer(
                    pixelValue, regions, lossyOutput, markCleanPixelData);

            AtomicReference<MaskMultiFrameUSTransformer.Result> result = new AtomicReference<>();
            long start = System.nanoTime();
            flowFile = session.write(flowFile, (in, out) -> result.set(transformer.transform(in, out)));
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            Map<String, String> attributes = new HashMap<>();
            attributes.put(ATTR_SOURCE_TS, result.get().getSourceTransferSyntax());
            attributes.put(ATTR_OUTPUT_TS, result.get().getOutputTransferSyntax());
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.getProvenanceReporter().modifyContent(flowFile, elapsedMillis);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Ultrasound-Multiframe-DICOM konnte nicht maskiert werden: {}", flowFile, e);
            flowFile = session.putAttribute(flowFile, ATTR_ERROR,
                    e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    static int parseColor(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Pixel Value darf nicht leer sein");
        }
        String normalized = value.trim();
        try {
            if (normalized.startsWith("#")) {
                return checkedColor(Long.parseLong(normalized.substring(1), 16));
            }
            if (normalized.startsWith("0x") || normalized.startsWith("0X")) {
                return checkedColor(Long.parseLong(normalized.substring(2), 16));
            }
            return checkedColor(Long.parseLong(normalized, 10));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Ungültiger Pixelwert: " + value, e);
        }
    }

    private static int checkedColor(long color) {
        if (color < 0 || color > 0xFFFFFF) {
            throw new IllegalArgumentException("Pixelwert muss zwischen 0 und 0xFFFFFF liegen");
        }
        return (int) color;
    }
}
