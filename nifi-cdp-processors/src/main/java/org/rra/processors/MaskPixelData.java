package org.rra.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.rra.dcm.MaskPxDataTransformer;
import org.rra.dcm.MaskRegion;
import org.rra.dcm.RegionMaskParser;

import java.io.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@SystemResourceConsideration(resource = SystemResource.CPU)

@Tags({"DICOM", "MaskPixel", "CDP"})
@CapabilityDescription("Change the pixel value of rectangular regions of uncompressed DICOM images to a particular value.")
@UseCase(description = "It may be used to mask (black out) information burned into the Pixel Data.",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)

public class MaskPixelData extends AbstractProcessor {

    private static int parseColor(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "Pixel Value darf nicht leer sein");
        }

        String normalized = value.trim();
        long color;

        try {
            if (normalized.startsWith("0x")
                    || normalized.startsWith("0X")) {

                color = Long.parseLong(
                        normalized.substring(2),
                        16
                );

            } else if (normalized.startsWith("#")) {
                color = Long.parseLong(
                        normalized.substring(1),
                        16
                );

            } else {
                color = Long.parseLong(
                        normalized,
                        10
                );
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Ungültiger Pixelwert: " + value
                            + ". Erlaubt sind beispielsweise "
                            + "0, 16711680, 0xff0000 oder #ff0000.",
                    e
            );
        }

        if (color < 0 || color > 0xFFFFFF) {
            throw new IllegalArgumentException(
                    "Pixelwert muss zwischen 0 und 0xFFFFFF liegen");
        }

        return (int) color;
    }
    private static final Validator REGIONS_VALIDATOR =
            (subject, input, validationContext) -> {

                try {
                    RegionMaskParser.parse(input);

                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(true)
                            .build();

                } catch (Exception e) {
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(false)
                            .explanation(e.getMessage())
                            .build();
                }
            };

    private static final Validator COLOR_VALIDATOR =
            (subject, input, context) -> {
                try {
                    parseColor(input);

                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(true)
                            .build();

                } catch (IllegalArgumentException e) {
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(false)
                            .explanation(e.getMessage())
                            .build();
                }
            };
    public static final PropertyDescriptor REGIONS = new PropertyDescriptor.Builder()
            .name("regions")
            .displayName("Regions")
            .description(
                    "Rectangular regions in the format "
                            + "[x,y,width,height]. Multiple regions "
                            + "are separated by commas, for example "
                            + "[10,20,100,50],[300,150,80,40]."
            )
            .required(true)
            .addValidator(REGIONS_VALIDATOR)
            .build();

    public static final PropertyDescriptor COLOR =
            new PropertyDescriptor.Builder()
                    .name("color")
                    .displayName("Pixel Value")
                    .description(
                            "Pixel value used to fill the configured regions. "
                                    + "For grayscale images (SamplesPerPixel=1), "
                                    + "enter a grayscale value appropriate for BitsStored: "
                                    + "0-255 for 8-bit, 0-4095 for 12-bit, or "
                                    + "0-65535 for 16-bit images. "
                                    + "For MONOCHROME2, 0 is normally black and the "
                                    + "maximum value is white; MONOCHROME1 is reversed. "
                                    + "For 8-bit RGB images (SamplesPerPixel=3), enter "
                                    + "a decimal RGB value or hexadecimal value such as "
                                    + "0xFF0000 for red. RGB colors cannot be applied "
                                    + "to grayscale images."
                    )
                    .required(true)
                    .defaultValue("0")
                    .addValidator(COLOR_VALIDATOR)
                    .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Sending success relationship of the transformer")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to transform").build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(REGIONS, COLOR);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        String regions = context.getProperty(REGIONS).getValue();
        int color = context.getProperty(COLOR).asInteger();
        getLogger().debug(
                "Masking regions {} using pixel value {}",
                regions,
                color
        );
        try {
            List<MaskRegion> aRegions = RegionMaskParser.parse(regions);
            MaskPxDataTransformer transformer = new MaskPxDataTransformer(
                    color,
                    aRegions
            );
            final long t1 = System.nanoTime();
            flowFile = session.write(
                    flowFile,
                    transformer::transform
            );
            final long importNanos = System.nanoTime() - t1;
            final long transcodeMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);
            session.getProvenanceReporter().modifyContent(flowFile, transcodeMillis);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (IllegalArgumentException e) {
            getLogger().error(
                    "Ungültige Maskierungsregion für {}: {}",
                    flowFile,
                    e.getMessage()
            );
            flowFile = session.putAttribute(flowFile,"maskpixel.error", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        } catch (ProcessException e) {
            getLogger().error(
                    "DICOM PixelData konnte für {} nicht maskiert werden",
                    flowFile,
                    e
            );
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
