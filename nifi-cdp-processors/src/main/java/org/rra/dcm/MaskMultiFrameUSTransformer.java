package org.rra.dcm;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Sequence;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.imageio.codec.Transcoder;
import org.dcm4che3.imageio.codec.TransferSyntaxType;
import org.dcm4che3.io.DicomEncodingOptions;
import org.dcm4che3.io.DicomInputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Masks Ultrasound Multi-frame instances.
 *
 * <p>Ablauf: Eingang dekodieren (Explicit VR Little Endian), alle Frames
 * maskieren, anschliessend wieder kodieren.</p>
 *
 * <ul>
 *   <li>Verlustfreier Eingang (nativ, RLE, JPEG Lossless, JPEG-LS Lossless,
 *       JPEG 2000 Lossless) wird im Original-Transfer-Syntax ausgegeben.</li>
 *   <li>Verlustbehafteter Eingang (JPEG Baseline/Extended, JPEG-LS
 *       Near-Lossless, JPEG 2000) wird gemäss {@link LossyOutput} ausgegeben,
 *       standardmässig verlustfrei als RLE Lossless.</li>
 * </ul>
 *
 * <p>Nach PS3.3 C.7.6.1.1.5 bleibt eine einmal verlustbehaftet komprimierte
 * Instanz als solche gekennzeichnet: Lossy Image Compression (0028,2110)
 * bleibt "01", Ratio und Method werden übernommen bzw. ergänzt und bei
 * erneuter verlustbehafteter Kodierung um den neuen Schritt erweitert.</p>
 */
public final class MaskMultiFrameUSTransformer {

    public static final String ULTRASOUND_MULTI_FRAME_SOP_CLASS_UID =
            "1.2.840.10008.5.1.4.1.1.3.1";

    static final String CLEAN_PIXEL_DATA_CODE_VALUE = "113101";
    static final String CLEAN_PIXEL_DATA_CODE_MEANING = "Clean Pixel Data Option";

    private static final Set<String> SUPPORTED_TRANSFER_SYNTAXES = Set.of(
            UID.ImplicitVRLittleEndian,
            UID.ExplicitVRLittleEndian,
            UID.ExplicitVRBigEndian,
            UID.JPEGBaseline8Bit,
            UID.JPEGExtended12Bit,
            UID.JPEGLossless,
            UID.JPEGLosslessSV1,
            UID.JPEGLSLossless,
            UID.JPEGLSNearLossless,
            UID.JPEG2000Lossless,
            UID.JPEG2000,
            UID.RLELossless
    );

    /**
     * Ziel-Transfer-Syntax für verlustbehaftet komprimierte Eingänge.
     */
    public enum LossyOutput {
        /** Verlustfrei, breit unterstützt (Default). */
        RLE_LOSSLESS(UID.RLELossless),
        /** Verlustfrei, beste Kompression; benötigt einen JPEG-LS-Writer (opencv). */
        JPEG_LS_LOSSLESS(UID.JPEGLSLossless),
        /** Unkomprimiert, maximal kompatibel, gross. */
        EXPLICIT_VR_LITTLE_ENDIAN(UID.ExplicitVRLittleEndian),
        /** Original-TS beibehalten, d.h. erneut verlustbehaftet kodieren. */
        ORIGINAL(null);

        private final String transferSyntax;

        LossyOutput(String transferSyntax) {
            this.transferSyntax = transferSyntax;
        }

        public String transferSyntax(String original) {
            return transferSyntax != null ? transferSyntax : original;
        }
    }

    /**
     * Ergebnis einer Transformation.
     */
    public static final class Result {
        private final String sourceTransferSyntax;
        private final String outputTransferSyntax;
        private final boolean lossySource;

        Result(String sourceTransferSyntax, String outputTransferSyntax, boolean lossySource) {
            this.sourceTransferSyntax = sourceTransferSyntax;
            this.outputTransferSyntax = outputTransferSyntax;
            this.lossySource = lossySource;
        }

        public String getSourceTransferSyntax() {
            return sourceTransferSyntax;
        }

        public String getOutputTransferSyntax() {
            return outputTransferSyntax;
        }

        public boolean isLossySource() {
            return lossySource;
        }
    }

    private final int pixelValue;
    private final List<MaskRegion> regions;
    private final LossyOutput lossyOutput;
    private final boolean markCleanPixelData;

    /**
     * @param pixelValue Grauwert bzw. RGB-Wert 0xRRGGBB. Bei YBR_FULL-Daten
     *                   wird ein RGB-Wert automatisch nach YBR umgerechnet.
     */
    public MaskMultiFrameUSTransformer(int pixelValue, List<MaskRegion> regions,
                                       LossyOutput lossyOutput, boolean markCleanPixelData) {
        if (regions == null || regions.isEmpty()) {
            throw new IllegalArgumentException("Mindestens eine Maskierungsregion ist erforderlich");
        }
        if (lossyOutput == null) {
            throw new IllegalArgumentException("lossyOutput darf nicht null sein");
        }
        this.pixelValue = pixelValue;
        this.regions = List.copyOf(regions);
        this.lossyOutput = lossyOutput;
        this.markCleanPixelData = markCleanPixelData;
    }

    public MaskMultiFrameUSTransformer(int pixelValue, List<MaskRegion> regions) {
        this(pixelValue, regions, LossyOutput.RLE_LOSSLESS, false);
    }

    /**
     * Maskiert eine Multi-frame-Ultraschallinstanz. Die Streams gehören
     * weiterhin dem Aufrufer.
     */
    public Result transform(InputStream input, OutputStream output) throws IOException {
        List<File> tempFiles = new ArrayList<>();
        try {
            File source = newTempFile(tempFiles, "source");
            File decoded = newTempFile(tempFiles, "native");
            File masked = newTempFile(tempFiles, "masked");
            File encoded = newTempFile(tempFiles, "encoded");

            Files.copy(input, source.toPath(), StandardCopyOption.REPLACE_EXISTING);
            SourceInfo info = inspectSource(source);
            String targetTransferSyntax = info.lossy
                    ? lossyOutput.transferSyntax(info.transferSyntax)
                    : info.transferSyntax;

            decode(source, decoded, info);

            int effectivePixelValue = effectivePixelValue(decoded);
            boolean pixelDataMasked;
            try (InputStream decodedInput = new FileInputStream(decoded);
                 OutputStream maskedOutput = new FileOutputStream(masked)) {
                pixelDataMasked = new MaskPxDataTransformer(effectivePixelValue, regions)
                        .transform(decodedInput, maskedOutput);
            }
            if (!pixelDataMasked) {
                throw new IOException("DICOM PixelData wurde nicht maskiert");
            }

            encode(masked, encoded, targetTransferSyntax);

            EncodedInfo encodedInfo = inspectEncoded(encoded);
            if (TransferSyntaxType.isLossyCompression(encodedInfo.transferSyntax)) {
                // Erneut verlustbehaftet kodiert: neuen Kompressionsschritt
                // an die Historie anhängen (PS3.3 C.7.6.1.1.5).
                appendLossyStep(encoded, output, info, encodedInfo);
            } else {
                Files.copy(encoded.toPath(), output);
            }
            return new Result(info.transferSyntax, encodedInfo.transferSyntax, info.lossy);
        } finally {
            for (File file : tempFiles) {
                Files.deleteIfExists(file.toPath());
            }
        }
    }

    // ------------------------------------------------------------------
    // Quelle prüfen
    // ------------------------------------------------------------------

    /**
     * Prüft anhand des Headers, ob dieser Transformer für die Instanz
     * zuständig ist. Liefert {@code null}, wenn ja, sonst den Grund.
     *
     * <p>"Nicht zuständig" (andere SOP Class, Single-Frame) ist kein Fehler.
     * Eine Ultraschall-Multiframe-Instanz, die nicht verarbeitet werden kann
     * (z.B. nicht freigegebene Transfer-Syntax), ist dagegen ein Fehler und
     * wird von {@link #transform} als IOException gemeldet, damit sie nicht
     * unmaskiert weiterläuft.</p>
     *
     * @throws IOException wenn der Inhalt kein lesbares DICOM ist
     */
    public static String unsupportedReason(InputStream input) throws IOException {
        DicomInputStream dis = new DicomInputStream(input);
        dis.readFileMetaInformation();
        return unsupportedReason(dis.readDatasetUntilPixelData());
    }

    static String unsupportedReason(Attributes dataset) {
        String sopClassUid = dataset.getString(Tag.SOPClassUID);
        if (!ULTRASOUND_MULTI_FRAME_SOP_CLASS_UID.equals(sopClassUid)) {
            return "SOP Class " + sopClassUid + " ist keine Ultrasound Multi-frame Image Storage";
        }
        if (dataset.getInt(Tag.NumberOfFrames, 1) <= 1) {
            return "Ultraschall-Single-Frame";
        }
        return null;
    }

    private static SourceInfo inspectSource(File source) throws IOException {
        try (DicomInputStream dis = new DicomInputStream(source)) {
            dis.readFileMetaInformation();
            Attributes dataset = dis.readDatasetUntilPixelData();

            String unsupported = unsupportedReason(dataset);
            if (unsupported != null) {
                throw new IOException("Nicht zuständig: " + unsupported);
            }
            int frames = dataset.getInt(Tag.NumberOfFrames, 1);

            String transferSyntax = dis.getTransferSyntax();
            if (transferSyntax == null) {
                transferSyntax = UID.ImplicitVRLittleEndian;
            }
            if (!SUPPORTED_TRANSFER_SYNTAXES.contains(transferSyntax)) {
                throw new IOException("Nicht freigegebene Transfer-Syntax für Ultraschall-Multiframe: "
                        + transferSyntax);
            }
            if (dis.tag() != Tag.PixelData) {
                throw new IOException("DICOM-Instanz enthält kein PixelData-Element");
            }

            long uncompressedBytes = (long) dataset.getInt(Tag.Rows, 0)
                    * dataset.getInt(Tag.Columns, 0)
                    * dataset.getInt(Tag.SamplesPerPixel, 1)
                    * ((dataset.getInt(Tag.BitsAllocated, 8) + 7) / 8)
                    * frames;

            boolean lossy = TransferSyntaxType.isLossyCompression(transferSyntax);
            String[] priorRatios = nonEmpty(dataset.getStrings(Tag.LossyImageCompressionRatio));
            String[] priorMethods = nonEmpty(dataset.getStrings(Tag.LossyImageCompressionMethod));

            if (lossy) {
                // Fehlende Angaben zum aktuellen (lossy) Kodierschritt ergänzen.
                if (priorRatios.length == 0) {
                    long compressedBytes = source.length() - dis.getPosition();
                    String ratio = formatRatio(uncompressedBytes, compressedBytes);
                    if (ratio != null) {
                        priorRatios = new String[]{ratio};
                    }
                }
                if (priorMethods.length == 0) {
                    String method = lossyMethod(transferSyntax);
                    if (method != null) {
                        priorMethods = new String[]{method};
                    }
                }
            }
            return new SourceInfo(transferSyntax, lossy, uncompressedBytes,
                    priorRatios, priorMethods);
        }
    }

    // ------------------------------------------------------------------
    // Dekodieren + Attribute aktualisieren
    // ------------------------------------------------------------------

    private void decode(File source, File target, SourceInfo info) throws IOException {
        Consumer<Attributes> customizer = dataset -> {
            applyLossyHistory(dataset, info);
            if (markCleanPixelData) {
                markCleanPixelData(dataset);
            }
        };
        if (UID.JPEGBaseline8Bit.equals(info.transferSyntax)) {
            // JPEG Baseline mit dem JDK-Codec statt opencv: der opencv-Codec
            // scheitert je nach nativer Bibliothek an Multiframe-JPEG
            // ("No implementation to handle 9 channels").
            try (InputStream in = new BufferedInputStream(new FileInputStream(source));
                 OutputStream out = new BufferedOutputStream(new FileOutputStream(target))) {
                JpegBaselineCodec.decode(in, out, customizer);
            }
            return;
        }
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(target));
             Transcoder transcoder = new Transcoder(
                     new BufferedInputStream(new FileInputStream(source)))) {
            transcoder.setIncludeFileMetaInformation(true);
            transcoder.setRetainFileMetaInformation(false);
            transcoder.setEncodingOptions(DicomEncodingOptions.DEFAULT);
            transcoder.setDestinationTransferSyntax(UID.ExplicitVRLittleEndian);
            // Der Handler wird vor dem Schreiben des Datasets aufgerufen;
            // dcm4che passt danach nur noch PhotometricInterpretation/
            // PlanarConfiguration an (YBR -> RGB bei JPEG/J2K-Quellen).
            transcoder.transcode((t, dataset) -> {
                customizer.accept(dataset);
                return new NonClosingOutputStream(out);
            });
        }
    }

    static void applyLossyHistory(Attributes dataset, SourceInfo info) {
        if (!info.lossy) {
            return;
        }
        dataset.setString(Tag.LossyImageCompression, VR.CS, "01");
        if (info.priorRatios.length > 0) {
            dataset.setString(Tag.LossyImageCompressionRatio, VR.DS, info.priorRatios);
        }
        if (info.priorMethods.length > 0) {
            dataset.setString(Tag.LossyImageCompressionMethod, VR.CS, info.priorMethods);
        }
    }

    static void markCleanPixelData(Attributes dataset) {
        dataset.setString(Tag.BurnedInAnnotation, VR.CS, "NO");
        Sequence methods = dataset.getSequence(Tag.DeidentificationMethodCodeSequence);
        if (methods == null) {
            methods = dataset.newSequence(Tag.DeidentificationMethodCodeSequence, 1);
        }
        for (Attributes item : methods) {
            if (CLEAN_PIXEL_DATA_CODE_VALUE.equals(item.getString(Tag.CodeValue))
                    && "DCM".equals(item.getString(Tag.CodingSchemeDesignator))) {
                return;
            }
        }
        Attributes code = new Attributes(3);
        code.setString(Tag.CodeValue, VR.SH, CLEAN_PIXEL_DATA_CODE_VALUE);
        code.setString(Tag.CodingSchemeDesignator, VR.SH, "DCM");
        code.setString(Tag.CodeMeaning, VR.LO, CLEAN_PIXEL_DATA_CODE_MEANING);
        methods.add(code);
    }

    // ------------------------------------------------------------------
    // Maskierungswert passend zum Farbraum
    // ------------------------------------------------------------------

    private int effectivePixelValue(File decoded) throws IOException {
        try (DicomInputStream dis = new DicomInputStream(decoded)) {
            dis.readFileMetaInformation();
            Attributes dataset = dis.readDatasetUntilPixelData();
            if (dataset.getInt(Tag.SamplesPerPixel, 1) != 3) {
                return pixelValue;
            }
            String pmi = dataset.getString(Tag.PhotometricInterpretation, "");
            switch (pmi) {
                case "RGB":
                    return pixelValue;
                case "YBR_FULL":
                    return rgbToYbrFull(pixelValue);
                default:
                    throw new IOException("Photometric Interpretation nach dem Dekodieren nicht unterstützt: "
                            + pmi + " (erwartet RGB oder YBR_FULL)");
            }
        }
    }

    /**
     * RGB (0xRRGGBB) nach YBR_FULL (0xYYCBCR) gemäss PS3.3 C.7.6.3.1.2.
     */
    static int rgbToYbrFull(int rgb) {
        int r = (rgb >> 16) & 0xFF;
        int g = (rgb >> 8) & 0xFF;
        int b = rgb & 0xFF;
        int y = clamp(Math.round(0.299f * r + 0.587f * g + 0.114f * b));
        int cb = clamp(Math.round(-0.168736f * r - 0.331264f * g + 0.5f * b + 128f));
        int cr = clamp(Math.round(0.5f * r - 0.418688f * g - 0.081312f * b + 128f));
        return (y << 16) | (cb << 8) | cr;
    }

    private static int clamp(int value) {
        return Math.max(0, Math.min(255, value));
    }

    // ------------------------------------------------------------------
    // Kodieren
    // ------------------------------------------------------------------

    private static void encode(File nativeSource, File target, String transferSyntax)
            throws IOException {
        try (InputStream in = new BufferedInputStream(new FileInputStream(nativeSource));
             OutputStream out = new BufferedOutputStream(new FileOutputStream(target))) {
            if (UID.RLELossless.equals(transferSyntax)) {
                // dcm4che hat keinen RLE-Writer
                RleLosslessEncoder.encode(in, out);
            } else if (UID.JPEGBaseline8Bit.equals(transferSyntax)) {
                // JDK-Codec statt opencv, siehe decode()
                JpegBaselineCodec.encode(in, out, JpegBaselineCodec.DEFAULT_QUALITY);
            } else {
                Dicom2DicomTranscoder.transcode(in, out, transferSyntax);
            }
        }
    }

    // ------------------------------------------------------------------
    // Erneute lossy-Kodierung dokumentieren
    // ------------------------------------------------------------------

    private static EncodedInfo inspectEncoded(File encoded) throws IOException {
        try (DicomInputStream dis = new DicomInputStream(encoded)) {
            dis.readFileMetaInformation();
            dis.readDatasetUntilPixelData();
            String ts = dis.getTransferSyntax();
            return new EncodedInfo(ts, encoded.length() - dis.getPosition());
        }
    }

    private static void appendLossyStep(File encoded, OutputStream output,
                                        SourceInfo info, EncodedInfo encodedInfo) throws IOException {
        String newRatio = formatRatio(info.uncompressedBytes, encodedInfo.pixelDataBytes);
        String newMethod = lossyMethod(encodedInfo.transferSyntax);

        String[] ratios = newRatio == null
                ? info.priorRatios : append(info.priorRatios, newRatio);
        String[] methods = newMethod == null
                ? info.priorMethods : append(info.priorMethods, newMethod);

        // Gleicher Transfer-Syntax: dcm4che kopiert die Fragmente unverändert,
        // nur die Attribute werden neu geschrieben.
        try (Transcoder transcoder = new Transcoder(
                new BufferedInputStream(new FileInputStream(encoded)))) {
            transcoder.setIncludeFileMetaInformation(true);
            transcoder.setRetainFileMetaInformation(false);
            transcoder.setEncodingOptions(DicomEncodingOptions.DEFAULT);
            transcoder.transcode((t, dataset) -> {
                dataset.setString(Tag.LossyImageCompression, VR.CS, "01");
                if (ratios.length > 0) {
                    dataset.setString(Tag.LossyImageCompressionRatio, VR.DS, ratios);
                }
                if (methods.length > 0) {
                    dataset.setString(Tag.LossyImageCompressionMethod, VR.CS, methods);
                }
                return new NonClosingOutputStream(output);
            });
        }
    }

    // ------------------------------------------------------------------
    // Hilfsfunktionen
    // ------------------------------------------------------------------

    /**
     * Defined Terms für Lossy Image Compression Method (0028,2114).
     */
    static String lossyMethod(String transferSyntax) {
        switch (transferSyntax) {
            case UID.JPEGBaseline8Bit:
            case UID.JPEGExtended12Bit:
                return "ISO_10918_1";
            case UID.JPEGLSNearLossless:
                return "ISO_14495_1";
            case UID.JPEG2000:
                return "ISO_15444_1";
            default:
                return null;
        }
    }

    static String formatRatio(long uncompressedBytes, long compressedBytes) {
        if (uncompressedBytes <= 0 || compressedBytes <= 0) {
            return null;
        }
        // DS: max. 16 Zeichen
        return String.format(Locale.ROOT, "%.2f", (double) uncompressedBytes / compressedBytes);
    }

    private static String[] nonEmpty(String[] values) {
        if (values == null) {
            return new String[0];
        }
        return Arrays.stream(values)
                .filter(v -> v != null && !v.isBlank())
                .toArray(String[]::new);
    }

    private static String[] append(String[] values, String value) {
        String[] result = Arrays.copyOf(values, values.length + 1);
        result[values.length] = value;
        return result;
    }

    private static File newTempFile(List<File> registry, String purpose) throws IOException {
        File file = Files.createTempFile("mask-multiframe-us-" + purpose + "-", ".dcm").toFile();
        registry.add(file);
        return file;
    }

    static final class SourceInfo {
        final String transferSyntax;
        final boolean lossy;
        final long uncompressedBytes;
        final String[] priorRatios;
        final String[] priorMethods;

        SourceInfo(String transferSyntax, boolean lossy, long uncompressedBytes,
                   String[] priorRatios, String[] priorMethods) {
            this.transferSyntax = transferSyntax;
            this.lossy = lossy;
            this.uncompressedBytes = uncompressedBytes;
            this.priorRatios = priorRatios;
            this.priorMethods = priorMethods;
        }
    }

    private static final class EncodedInfo {
        final String transferSyntax;
        final long pixelDataBytes;

        EncodedInfo(String transferSyntax, long pixelDataBytes) {
            this.transferSyntax = transferSyntax;
            this.pixelDataBytes = pixelDataBytes;
        }
    }

    /**
     * Verhindert, dass dcm4che den Ziel-Stream schliesst.
     */
    private static final class NonClosingOutputStream extends java.io.FilterOutputStream {
        NonClosingOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
            flush();
        }
    }
}
