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
import org.dcm4che3.io.DicomOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * Masks Ultrasound Multi-frame instances.
 *
 * <p>Alle Frames werden dekodiert, maskiert und wieder kodiert.</p>
 *
 * <ul>
 *   <li>Verlustfreier Eingang (nativ, RLE, JPEG Lossless, JPEG-LS Lossless,
 *       JPEG 2000 Lossless) wird im Original-Transfer-Syntax ausgegeben.</li>
 *   <li>Verlustbehafteter Eingang (JPEG Baseline/Extended, JPEG-LS
 *       Near-Lossless, JPEG 2000) wird gemäss {@link LossyOutput} ausgegeben,
 *       standardmässig verlustfrei als RLE Lossless.</li>
 * </ul>
 *
 * <p><b>Ein-Durchlauf-Pfad:</b> Für nativen Little-Endian-, RLE- und
 * JPEG-Baseline-Eingang mit nativem, RLE- oder JPEG-Baseline-Ziel wird jeder
 * Frame direkt aus dem Eingang gelesen, im Speicher maskiert und in den
 * Ausgang geschrieben, ohne temporäre Dateien. Optional werden die Frames
 * parallel verarbeitet. Alle übrigen Kombinationen laufen über dcm4che und
 * temporäre Dateien.</p>
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

    private static final int IO_BUFFER = 1 << 16;

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

    /** Transfer-Syntaxen, die der Ein-Durchlauf-Pfad lesen und schreiben kann. */
    private static final Set<String> STREAMING_TRANSFER_SYNTAXES = Set.of(
            UID.ImplicitVRLittleEndian,
            UID.ExplicitVRLittleEndian,
            UID.RLELossless,
            UID.JPEGBaseline8Bit
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
    private final ExecutorService frameExecutor;
    private final int frameThreads;

    /**
     * @param pixelValue    Grauwert bzw. RGB-Wert 0xRRGGBB. Bei YBR_FULL-Daten
     *                      wird ein RGB-Wert automatisch nach YBR umgerechnet.
     * @param frameExecutor optionaler Pool für parallele Frame-Verarbeitung
     *                      im Ein-Durchlauf-Pfad; null = sequentiell
     * @param frameThreads  Anzahl Threads des Pools (bestimmt die Anzahl
     *                      gleichzeitig gehaltener Frames)
     */
    public MaskMultiFrameUSTransformer(int pixelValue, List<MaskRegion> regions,
                                       LossyOutput lossyOutput, boolean markCleanPixelData,
                                       ExecutorService frameExecutor, int frameThreads) {
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
        this.frameExecutor = frameThreads > 1 ? frameExecutor : null;
        this.frameThreads = Math.max(1, frameThreads);
    }

    public MaskMultiFrameUSTransformer(int pixelValue, List<MaskRegion> regions,
                                       LossyOutput lossyOutput, boolean markCleanPixelData) {
        this(pixelValue, regions, lossyOutput, markCleanPixelData, null, 1);
    }

    public MaskMultiFrameUSTransformer(int pixelValue, List<MaskRegion> regions) {
        this(pixelValue, regions, LossyOutput.RLE_LOSSLESS, false);
    }

    /**
     * Maskiert eine Multi-frame-Ultraschallinstanz. Die Streams gehören
     * weiterhin dem Aufrufer.
     */
    public Result transform(InputStream input, OutputStream output) throws IOException {
        // Der Header wird genau einmal gelesen. Er kann sehr gross sein
        // (z.B. private Rohdaten von Ultraschallgeräten vor PixelData),
        // daher kein mark/reset mit fester Obergrenze.
        DicomInputStream dis = new DicomInputStream(
                new BufferedInputStream(new NonClosingInputStream(input), IO_BUFFER));
        Attributes fmi = dis.readFileMetaInformation();
        Attributes dataset = dis.readDatasetUntilPixelData();
        String sourceTs = validate(dis, dataset);
        boolean lossy = TransferSyntaxType.isLossyCompression(sourceTs);
        String targetTs = lossy ? lossyOutput.transferSyntax(sourceTs) : sourceTs;

        OutputStream out = new BufferedOutputStream(new NonClosingOutputStream(output), IO_BUFFER);
        Result result;
        if (STREAMING_TRANSFER_SYNTAXES.contains(sourceTs) && STREAMING_TRANSFER_SYNTAXES.contains(targetTs)) {
            result = transformStreaming(dis, dataset, sourceTs, out, targetTs);
        } else {
            result = transformViaTempFiles(dis, fmi, dataset, sourceTs, out);
        }
        out.flush();
        return result;
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

    /**
     * @return Transfer-Syntax der Quelle
     */
    private static String validate(DicomInputStream dis, Attributes dataset) throws IOException {
        String unsupported = unsupportedReason(dataset);
        if (unsupported != null) {
            throw new IOException("Nicht zuständig: " + unsupported);
        }
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
        return transferSyntax;
    }

    static SourceInfo sourceInfo(Attributes dataset, String transferSyntax, long compressedBytes) {
        long uncompressedBytes = (long) dataset.getInt(Tag.Rows, 0)
                * dataset.getInt(Tag.Columns, 0)
                * dataset.getInt(Tag.SamplesPerPixel, 1)
                * ((dataset.getInt(Tag.BitsAllocated, 8) + 7) / 8)
                * dataset.getInt(Tag.NumberOfFrames, 1);

        boolean lossy = TransferSyntaxType.isLossyCompression(transferSyntax);
        String[] priorRatios = nonEmpty(dataset.getStrings(Tag.LossyImageCompressionRatio));
        String[] priorMethods = nonEmpty(dataset.getStrings(Tag.LossyImageCompressionMethod));
        if (lossy) {
            // Fehlende Angaben zum aktuellen (lossy) Kodierschritt ergänzen.
            if (priorRatios.length == 0) {
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
        return new SourceInfo(transferSyntax, lossy, uncompressedBytes, priorRatios, priorMethods);
    }

    // ------------------------------------------------------------------
    // Ein-Durchlauf-Pfad
    // ------------------------------------------------------------------

    private Result transformStreaming(DicomInputStream dis, Attributes dataset, String sourceTs,
                                      OutputStream out, String targetTs) throws IOException {

        int rows = dataset.getInt(Tag.Rows, 0);
        int columns = dataset.getInt(Tag.Columns, 0);
        int samples = dataset.getInt(Tag.SamplesPerPixel, 1);
        int bitsAllocated = dataset.getInt(Tag.BitsAllocated, 0);
        int frames = dataset.getInt(Tag.NumberOfFrames, 1);
        int bytesPerSample = (bitsAllocated + 7) / 8;
        if (rows <= 0 || columns <= 0) {
            throw new IOException("Ungültige Bildgeometrie: " + columns + "x" + rows);
        }
        long frameSizeLong = (long) rows * columns * samples * bytesPerSample;
        if (frameSizeLong > Integer.MAX_VALUE - 64) {
            throw new IOException("Ein einzelnes Frame ist zu gross: " + frameSizeLong + " Bytes");
        }
        int frameSize = (int) frameSizeLong;

        boolean nativeSource = !UID.RLELossless.equals(sourceTs) && !UID.JPEGBaseline8Bit.equals(sourceTs);
        boolean jpegSource = UID.JPEGBaseline8Bit.equals(sourceTs);
        if (nativeSource == (dis.length() == -1)) {
            throw new IOException("PixelData passt nicht zur Transfer-Syntax " + sourceTs);
        }
        if (jpegSource) {
            JpegBaselineCodec.checkFormat(bitsAllocated, samples);
        }

        // Quelle: native Frames werden gestreamt, RLE-Fragmente ebenfalls
        // (ein Fragment pro Frame), JPEG-Fragmente werden vorab gelesen
        // (komprimiert, klein; nötig für die Kompressionsrate).
        long nativePixelLength = 0;
        List<byte[]> jpegFrames = null;
        long compressedBytes = 0;
        if (nativeSource) {
            nativePixelLength = dis.unsignedLength();
            if (nativePixelLength < frameSizeLong * frames) {
                throw new IOException("PixelData ist kleiner als erwartet: vorhanden=" + nativePixelLength
                        + ", erwartet=" + frameSizeLong * frames);
            }
        } else {
            byte[] offsetTable = JpegBaselineCodec.readItem(dis);
            if (offsetTable == null) {
                throw new IOException("PixelData enthält keine Offset-Tabelle");
            }
            if (jpegSource) {
                List<byte[]> fragments = new ArrayList<>();
                byte[] item;
                while ((item = JpegBaselineCodec.readItem(dis)) != null) {
                    fragments.add(item);
                    compressedBytes += item.length;
                }
                jpegFrames = new ArrayList<>(
                        JpegBaselineCodec.groupFragmentsToFrames(fragments, offsetTable, frames));
            }
        }
        SourceInfo info = sourceInfo(dataset, sourceTs, compressedBytes);

        // Layout der dekodierten Frames
        int decodedPlanar = nativeSource ? dataset.getInt(Tag.PlanarConfiguration, 0) : 0;
        String decodedPmi = jpegSource && samples == 3
                ? "RGB" : dataset.getString(Tag.PhotometricInterpretation, "");
        FrameMask mask = new FrameMask(regions, maskValueFor(samples, decodedPmi),
                rows, columns, samples, bytesPerSample, decodedPlanar);

        // Ausgabe-Attribute
        applyLossyHistory(dataset, info);
        if (markCleanPixelData) {
            markCleanPixelData(dataset);
        }
        if (samples == 3) {
            dataset.setString(Tag.PhotometricInterpretation, VR.CS, decodedPmi);
            dataset.setInt(Tag.PlanarConfiguration, VR.US, decodedPlanar);
        }
        boolean nativeTarget = !UID.RLELossless.equals(targetTs) && !UID.JPEGBaseline8Bit.equals(targetTs);
        boolean jpegTarget = UID.JPEGBaseline8Bit.equals(targetTs);
        if (jpegTarget) {
            JpegBaselineCodec.checkEncodable(dataset);
        }
        if (!nativeTarget && samples == 3) {
            dataset.setInt(Tag.PlanarConfiguration, VR.US, 0);
            if (jpegTarget) {
                dataset.setString(Tag.PhotometricInterpretation, VR.CS, "YBR_FULL_422");
            }
        }

        // Verarbeitung pro Frame: dekodieren -> maskieren -> kodieren
        final int planar = decodedPlanar;
        FrameWork work = unit -> {
            byte[] frame;
            if (nativeSource) {
                frame = unit;
            } else {
                frame = new byte[frameSize];
                if (jpegSource) {
                    JpegBaselineCodec.decodeFrame(unit, frame, columns, rows, samples);
                } else {
                    RleLosslessCodec.decodeFrame(unit, frame, rows, columns, samples, bytesPerSample);
                }
            }
            mask.apply(frame);
            if (nativeTarget) {
                return frame;
            }
            if (jpegTarget) {
                return JpegBaselineCodec.encodeFrame(frame, columns, rows, samples, planar,
                        JpegBaselineCodec.DEFAULT_QUALITY);
            }
            return RleLosslessCodec.encodeFrame(frame, rows, columns, samples, bytesPerSample, planar);
        };
        List<byte[]> jpegUnits = jpegFrames;
        FrameSource source = index -> {
            if (jpegUnits != null) {
                byte[] unit = jpegUnits.get(index);
                jpegUnits.set(index, null); // Speicher früh freigeben
                return unit;
            }
            if (nativeSource) {
                byte[] frame = new byte[frameSize];
                dis.readFully(frame);
                return frame;
            }
            byte[] fragment = JpegBaselineCodec.readItem(dis);
            if (fragment == null) {
                throw new IOException("RLE: weniger Fragmente als Frames (" + index + " von " + frames + ")");
            }
            return fragment;
        };

        DicomOutputStream dos = new DicomOutputStream(out, UID.ExplicitVRLittleEndian);
        if (jpegTarget) {
            // Erneut lossy: Kompressionsrate des neuen Schritts muss vor
            // PixelData im Header stehen, daher Fragmente erst sammeln.
            List<byte[]> encoded = new ArrayList<>(frames);
            processFrames(frames, source, work, encoded::add);
            long encodedBytes = 0;
            for (byte[] fragment : encoded) {
                encodedBytes += fragment.length;
            }
            appendLossyStep(dataset, info, targetTs, encodedBytes);
            dos.writeDataset(dataset.createFileMetaInformation(targetTs), dataset);
            dos.writeHeader(Tag.PixelData, VR.OB, -1);
            dos.writeHeader(Tag.Item, null, 0);
            for (byte[] fragment : encoded) {
                JpegBaselineCodec.writeFragment(dos, fragment);
            }
            dos.writeHeader(Tag.SequenceDelimitationItem, null, 0);
        } else if (nativeTarget) {
            long pixelLength = frameSizeLong * frames;
            if (pixelLength > 0xFFFFFFFEL) {
                throw new IOException("PixelData zu gross für natives Format: " + pixelLength + " Bytes");
            }
            dos.writeDataset(dataset.createFileMetaInformation(targetTs), dataset);
            dos.writeHeader(Tag.PixelData, bytesPerSample == 2 ? VR.OW : VR.OB,
                    (int) ((pixelLength + 1) & ~1L));
            processFrames(frames, source, work, dos::write);
            if ((pixelLength & 1) != 0) {
                dos.write(0);
            }
        } else {
            dos.writeDataset(dataset.createFileMetaInformation(targetTs), dataset);
            dos.writeHeader(Tag.PixelData, VR.OB, -1);
            dos.writeHeader(Tag.Item, null, 0);
            processFrames(frames, source, work, fragment -> JpegBaselineCodec.writeFragment(dos, fragment));
            dos.writeHeader(Tag.SequenceDelimitationItem, null, 0);
        }

        // Rest der Quelle nach den Frames: Padding bzw. Sequence Delimitation
        // Item, danach eventuelle Attribute hinter PixelData übernehmen.
        if (nativeSource) {
            long remaining = nativePixelLength - frameSizeLong * frames;
            if (remaining > 0) {
                dis.skipFully(remaining);
            }
        } else if (!jpegSource) {
            if (JpegBaselineCodec.readItem(dis) != null) {
                throw new IOException("RLE: mehr Fragmente als Frames (" + frames + ")");
            }
        }
        Attributes trailing = new Attributes(dis.bigEndian(), 4);
        dis.readAttributes(trailing, -1, -1);
        trailing.writeTo(dos);
        dos.finish();
        dos.flush();
        return new Result(sourceTs, targetTs, info.lossy);
    }

    /**
     * Liest, verarbeitet und schreibt alle Frames in Originalreihenfolge.
     * Mit Pool laufen bis zu 2 x frameThreads Frames gleichzeitig.
     */
    private void processFrames(int frames, FrameSource source, FrameWork work, FrameSink sink)
            throws IOException {
        if (frameExecutor == null) {
            for (int i = 0; i < frames; i++) {
                sink.accept(work.apply(source.read(i)));
            }
            return;
        }
        int window = 2 * frameThreads;
        Deque<Future<byte[]>> pending = new ArrayDeque<>(window);
        try {
            for (int i = 0; i < frames; i++) {
                byte[] unit = source.read(i);
                pending.add(frameExecutor.submit(() -> work.apply(unit)));
                if (pending.size() >= window) {
                    sink.accept(await(pending.poll()));
                }
            }
            while (!pending.isEmpty()) {
                sink.accept(await(pending.poll()));
            }
        } finally {
            for (Future<byte[]> future : pending) {
                future.cancel(true);
            }
        }
    }

    private static byte[] await(Future<byte[]> future) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Frame-Verarbeitung unterbrochen", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IOException(cause);
        }
    }

    @FunctionalInterface
    private interface FrameSource {
        byte[] read(int index) throws IOException;
    }

    @FunctionalInterface
    private interface FrameWork {
        byte[] apply(byte[] unit) throws IOException;
    }

    @FunctionalInterface
    private interface FrameSink {
        void accept(byte[] data) throws IOException;
    }

    // ------------------------------------------------------------------
    // Fallback über dcm4che und temporäre Dateien
    // ------------------------------------------------------------------

    private Result transformViaTempFiles(DicomInputStream dis, Attributes fmi, Attributes dataset,
                                         String sourceTs, OutputStream output) throws IOException {
        List<File> tempFiles = new ArrayList<>();
        try {
            File source = newTempFile(tempFiles, "source");
            File decoded = newTempFile(tempFiles, "native");
            File masked = newTempFile(tempFiles, "masked");
            File encoded = newTempFile(tempFiles, "encoded");

            long pixelDataBytes = spool(dis, fmi, dataset, sourceTs, source);
            SourceInfo info = sourceInfo(dataset, sourceTs, pixelDataBytes);
            String targetTransferSyntax = info.lossy
                    ? lossyOutput.transferSyntax(info.transferSyntax)
                    : info.transferSyntax;

            decode(source, decoded, info);

            int effectivePixelValue = effectivePixelValue(decoded);
            boolean pixelDataMasked;
            try (InputStream decodedInput = new BufferedInputStream(new FileInputStream(decoded), IO_BUFFER);
                 OutputStream maskedOutput = new BufferedOutputStream(new FileOutputStream(masked), IO_BUFFER)) {
                pixelDataMasked = new MaskPxDataTransformer(effectivePixelValue, regions)
                        .transform(decodedInput, maskedOutput);
            }
            if (!pixelDataMasked) {
                throw new IOException("DICOM PixelData wurde nicht maskiert");
            }

            encode(masked, encoded, targetTransferSyntax);

            String encodedTs;
            long encodedBytes;
            try (DicomInputStream encodedDis = new DicomInputStream(encoded)) {
                encodedDis.readFileMetaInformation();
                encodedDis.readDatasetUntilPixelData();
                encodedTs = encodedDis.getTransferSyntax();
                encodedBytes = encoded.length() - encodedDis.getPosition();
            }
            if (TransferSyntaxType.isLossyCompression(encodedTs)) {
                rewriteWithLossyStep(encoded, output, info, encodedTs, encodedBytes);
            } else {
                Files.copy(encoded.toPath(), output);
            }
            return new Result(info.transferSyntax, encodedTs, info.lossy);
        } finally {
            for (File file : tempFiles) {
                Files.deleteIfExists(file.toPath());
            }
        }
    }

    /**
     * Schreibt die bereits gelesene Instanz in eine Datei: Header aus den
     * gelesenen Attributen, PixelData und Rest unverändert aus dem Stream.
     *
     * @return Anzahl Bytes ab PixelData
     */
    private static long spool(DicomInputStream dis, Attributes fmi, Attributes dataset, String sourceTs,
                              File target) throws IOException {
        if (fmi == null) {
            fmi = dataset.createFileMetaInformation(sourceTs);
        }
        try (OutputStream file = new BufferedOutputStream(new FileOutputStream(target), IO_BUFFER);
             CountingOutputStream counter = new CountingOutputStream(file)) {
            DicomOutputStream dos = new DicomOutputStream(counter, UID.ExplicitVRLittleEndian);
            dos.writeDataset(fmi, dataset);
            dos.writeHeader(Tag.PixelData, dis.vr(), dis.length());
            dos.flush();
            long headerBytes = counter.count;
            byte[] buffer = new byte[IO_BUFFER];
            int read;
            while ((read = dis.read(buffer)) != -1) {
                counter.write(buffer, 0, read);
            }
            counter.flush();
            return counter.count - headerBytes;
        }
    }

    private static final class CountingOutputStream extends FilterOutputStream {
        long count;

        CountingOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            count++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            count += len;
        }
    }

    private void decode(File source, File target, SourceInfo info) throws IOException {
        Consumer<Attributes> customizer = dataset -> {
            applyLossyHistory(dataset, info);
            if (markCleanPixelData) {
                markCleanPixelData(dataset);
            }
        };
        if (UID.JPEGBaseline8Bit.equals(info.transferSyntax)) {
            // JDK-Codec statt opencv, siehe JpegBaselineCodec
            try (InputStream in = new BufferedInputStream(new FileInputStream(source), IO_BUFFER);
                 OutputStream out = new BufferedOutputStream(new FileOutputStream(target), IO_BUFFER)) {
                JpegBaselineCodec.decode(in, out, customizer);
            }
            return;
        }
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(target), IO_BUFFER);
             Transcoder transcoder = new Transcoder(
                     new BufferedInputStream(new FileInputStream(source), IO_BUFFER))) {
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

    private int effectivePixelValue(File decoded) throws IOException {
        try (DicomInputStream dis = new DicomInputStream(decoded)) {
            dis.readFileMetaInformation();
            Attributes dataset = dis.readDatasetUntilPixelData();
            return maskValueFor(dataset.getInt(Tag.SamplesPerPixel, 1),
                    dataset.getString(Tag.PhotometricInterpretation, ""));
        }
    }

    private static void encode(File nativeSource, File target, String transferSyntax)
            throws IOException {
        try (InputStream in = new BufferedInputStream(new FileInputStream(nativeSource), IO_BUFFER);
             OutputStream out = new BufferedOutputStream(new FileOutputStream(target), IO_BUFFER)) {
            if (UID.RLELossless.equals(transferSyntax)) {
                // dcm4che hat keinen RLE-Writer
                RleLosslessCodec.encode(in, out);
            } else if (UID.JPEGBaseline8Bit.equals(transferSyntax)) {
                // JDK-Codec statt opencv, siehe JpegBaselineCodec
                JpegBaselineCodec.encode(in, out, JpegBaselineCodec.DEFAULT_QUALITY);
            } else {
                Dicom2DicomTranscoder.transcode(in, out, transferSyntax);
            }
        }
    }

    private static void rewriteWithLossyStep(File encoded, OutputStream output, SourceInfo info,
                                             String encodedTs, long encodedBytes) throws IOException {
        // Gleicher Transfer-Syntax: dcm4che kopiert die Fragmente unverändert,
        // nur die Attribute werden neu geschrieben.
        try (Transcoder transcoder = new Transcoder(
                new BufferedInputStream(new FileInputStream(encoded), IO_BUFFER))) {
            transcoder.setIncludeFileMetaInformation(true);
            transcoder.setRetainFileMetaInformation(false);
            transcoder.setEncodingOptions(DicomEncodingOptions.DEFAULT);
            transcoder.transcode((t, dataset) -> {
                appendLossyStep(dataset, info, encodedTs, encodedBytes);
                return new NonClosingOutputStream(output);
            });
        }
    }

    // ------------------------------------------------------------------
    // Attribute
    // ------------------------------------------------------------------

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

    /**
     * Erneute lossy-Kodierung dokumentieren: neuen Schritt an Ratio und
     * Method anhängen (PS3.3 C.7.6.1.1.5).
     */
    static void appendLossyStep(Attributes dataset, SourceInfo info, String encodedTs, long encodedBytes) {
        String newRatio = formatRatio(info.uncompressedBytes, encodedBytes);
        String newMethod = lossyMethod(encodedTs);
        String[] ratios = newRatio == null ? info.priorRatios : append(info.priorRatios, newRatio);
        String[] methods = newMethod == null ? info.priorMethods : append(info.priorMethods, newMethod);
        dataset.setString(Tag.LossyImageCompression, VR.CS, "01");
        if (ratios.length > 0) {
            dataset.setString(Tag.LossyImageCompressionRatio, VR.DS, ratios);
        }
        if (methods.length > 0) {
            dataset.setString(Tag.LossyImageCompressionMethod, VR.CS, methods);
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

    private int maskValueFor(int samples, String photometricInterpretation) throws IOException {
        if (samples != 3) {
            return pixelValue;
        }
        switch (photometricInterpretation) {
            case "RGB":
                return pixelValue;
            case "YBR_FULL":
                return rgbToYbrFull(pixelValue);
            default:
                throw new IOException("Photometric Interpretation nach dem Dekodieren nicht unterstützt: "
                        + photometricInterpretation + " (erwartet RGB oder YBR_FULL)");
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

    /**
     * Verhindert, dass dcm4che den Stream des Aufrufers schliesst.
     */
    private static final class NonClosingInputStream extends FilterInputStream {
        NonClosingInputStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() {
            // Stream gehört dem Aufrufer
        }
    }

    /**
     * Verhindert, dass dcm4che den Ziel-Stream schliesst. Überschreibt
     * write(byte[],int,int), da FilterOutputStream sonst byteweise schreibt.
     */
    private static final class NonClosingOutputStream extends FilterOutputStream {
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
