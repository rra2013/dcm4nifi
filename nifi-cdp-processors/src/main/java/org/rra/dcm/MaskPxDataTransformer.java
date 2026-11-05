package org.rra.dcm;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Fragments;
import org.dcm4che3.data.Sequence;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.io.DicomInputHandler;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.util.ByteUtils;
import org.dcm4che3.util.StreamUtils;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;

public final class MaskPxDataTransformer {

    private static final int COPY_BUFFER_SIZE = 8192;

    private final int pxval;
    private final List<MaskRegion> regions;

    /**
     * @param pxval  Grauwert oder RGB-Wert, beispielsweise 0 oder 0xFF0000
     * @param regions Regionen im Format x, y, width, height
     */
    public MaskPxDataTransformer(int pxval, List<MaskRegion> regions) {
        this.pxval = pxval;
        this.regions = regions;

        if (regions.size() == 0) {
            throw new IllegalArgumentException(
                    "regions muss aus x,y,width,height-Objekte bestehen");
        }
    }

    /**
     * Liest eine DICOM-Datei aus dem InputStream, maskiert PixelData
     * und schreibt das Ergebnis in den OutputStream.
     *
     * Die Streams gehören weiterhin dem Aufrufer und werden nicht geschlossen.
     *
     * @return true, wenn PixelData gefunden und maskiert wurde
     */
    public boolean transform(
            InputStream input,
            OutputStream output
    ) throws IOException {

        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(output, "output");

        try (DicomInputStream dis =
                     new DicomInputStream(new NonClosingInputStream(input))) {

            Attributes fmi = dis.readFileMetaInformation();
            String transferSyntax = dis.getTransferSyntax();

            if (transferSyntax == null) {
                transferSyntax = UID.ImplicitVRLittleEndian;
            }

            /*
             * Wenn File Meta Information vorhanden ist, wird sie immer
             * Explicit VR Little Endian geschrieben. Der eigentliche
             * Dataset-Transfer-Syntax-Wechsel erfolgt anhand der FMI.
             */
            String initialOutputSyntax = fmi != null
                    ? UID.ExplicitVRLittleEndian
                    : transferSyntax;

            try (DicomOutputStream dos = new DicomOutputStream(
                    new NonClosingOutputStream(output),
                    initialOutputSyntax)) {

                Attributes dataset = new Attributes(dis.bigEndian(), 64);

                StreamingMaskHandler handler =
                        new StreamingMaskHandler(dos, fmi);

                dis.setDicomInputHandler(handler);
                dis.readAllAttributes(dataset);

                if (handler.isPixelDataProcessed()) {
                    /*
                     * Attribute, die nach PixelData stehen, wurden erst
                     * nach der Pixelverarbeitung eingelesen.
                     */
                    dataset.writePostPixelDataTo(dos);
                } else {
                    /*
                     * Kein PixelData vorhanden:
                     * Datei unverändert wieder schreiben.
                     */
                    dos.writeDataset(fmi, dataset);
                }

                dos.finish();
                dos.flush();

                return handler.isPixelDataProcessed();
            }
        }
    }

    private final class StreamingMaskHandler implements DicomInputHandler {

        private final DicomOutputStream dos;
        private final Attributes fmi;

        private boolean pixelDataProcessed;

        private StreamingMaskHandler(
                DicomOutputStream dos,
                Attributes fmi
        ) {
            this.dos = dos;
            this.fmi = fmi;
        }

        @Override
        public void readValue(
                DicomInputStream dis,
                Attributes attrs
        ) throws IOException {

            if (dis.level() == 0 && dis.tag() == Tag.PixelData) {
                processPixelData(dis, attrs);
                pixelDataProcessed = true;
            } else {
                dis.readValue(dis, attrs);
            }
        }

        @Override
        public void readValue(
                DicomInputStream dis,
                Sequence sequence
        ) throws IOException {
            dis.readValue(dis, sequence);
        }

        @Override
        public void readValue(
                DicomInputStream dis,
                Fragments fragments
        ) throws IOException {
            dis.readValue(dis, fragments);
        }

        @Override
        public void startDataset(DicomInputStream dis) {
            // Keine Aktion erforderlich
        }

        @Override
        public void endDataset(DicomInputStream dis) {
            // Keine Aktion erforderlich
        }

        private void processPixelData(
                DicomInputStream dis,
                Attributes attrs
        ) throws IOException {

            /*
             * Undefined Length bedeutet normalerweise encapsulated/compressed
             * PixelData, beispielsweise JPEG, JPEG-LS, JPEG 2000 oder RLE.
             */
            if (dis.length() == -1) {
                throw new IOException(
                        "Komprimierte/encapsulated PixelData wird nicht "
                                + "unterstützt. Die DICOM-Datei muss vor dem "
                                + "Maskieren dekomprimiert werden.");
            }

            long pixelDataLength = dis.unsignedLength();

            int rows = requiredPositive(attrs, Tag.Rows, "Rows");
            int columns = requiredPositive(attrs, Tag.Columns, "Columns");
            int frames = attrs.getInt(Tag.NumberOfFrames, 1);
            int samples = attrs.getInt(Tag.SamplesPerPixel, 1);
            int bitsAllocated = requiredPositive(
                    attrs,
                    Tag.BitsAllocated,
                    "BitsAllocated");

            if (frames <= 0) {
                throw new IOException(
                        "Ungültige NumberOfFrames: " + frames);
            }

            if (samples != 1 && samples != 3) {
                throw new IOException(
                        "SamplesPerPixel wird nicht unterstützt: " + samples);
            }

            if (bitsAllocated != 8 && bitsAllocated != 16) {
                throw new IOException(
                        "BitsAllocated wird nicht unterstützt: "
                                + bitsAllocated
                                + ". Unterstützt werden 8 und 16 Bit.");
            }

            if (samples == 3 && bitsAllocated != 8) {
                throw new IOException(
                        "Farbbilder werden aktuell nur mit "
                                + "8 BitsAllocated unterstützt.");
            }

            validateRegions(columns, rows);

            int bytesAllocated = bitsAllocated / 8;
            int planarConfiguration =
                    attrs.getInt(Tag.PlanarConfiguration, 0);

            long planeSizeLong =
                    Math.multiplyExact((long) rows, columns);

            long frameSizeLong = Math.multiplyExact(
                    Math.multiplyExact(planeSizeLong, samples),
                    bytesAllocated);

            long expectedPixelBytes =
                    Math.multiplyExact(frameSizeLong, frames);

            if (frameSizeLong > Integer.MAX_VALUE) {
                throw new IOException(
                        "Ein einzelnes Frame ist zu groß: "
                                + frameSizeLong + " Bytes");
            }

            if (pixelDataLength < expectedPixelBytes) {
                throw new IOException(
                        "PixelData ist kleiner als erwartet: vorhanden="
                                + pixelDataLength
                                + ", erwartet="
                                + expectedPixelBytes);
            }

            int planeSize = Math.toIntExact(planeSizeLong);
            int frameSize = Math.toIntExact(frameSizeLong);
            byte[] frameBuffer = new byte[frameSize];

            /*
             * Bis zu PixelData sind jetzt alle Attribute vorhanden.
             */
            dos.writeDataset(fmi, attrs);
            dos.writeHeader(
                    Tag.PixelData,
                    dis.vr(),
                    dis.length());

            for (int frame = 0; frame < frames; frame++) {
                dis.readFully(frameBuffer);

                maskFrame(
                        frameBuffer,
                        dis.bigEndian(),
                        columns,
                        planeSize,
                        samples,
                        bytesAllocated,
                        planarConfiguration);

                dos.write(frameBuffer);
            }

            /*
             * Übernimmt gegebenenfalls Padding oder zusätzliche Bytes,
             * ohne sie zu verändern.
             */
            long remaining =
                    pixelDataLength - expectedPixelBytes;

            if (remaining > 0) {
                StreamUtils.copy(
                        dis,
                        dos,
                        remaining,
                        new byte[COPY_BUFFER_SIZE]);
            }
        }

        private boolean isPixelDataProcessed() {
            return pixelDataProcessed;
        }
    }

    private void maskFrame(
            byte[] frame,
            boolean bigEndian,
            int columns,
            int planeSize,
            int samples,
            int bytesAllocated,
            int planarConfiguration
    ) {

        Mask mask;

        if (samples == 3) {
            mask = planarConfiguration == 0
                    ? this::maskColorByPixel
                    : this::maskColorByPlane;
        } else {
            mask = bytesAllocated == 1
                    ? this::maskByte
                    : this::maskWord;
        }

        regions.forEach(r -> {
            int x = effectiveX(r);
            int y = r.getY();
            int width = effectiveWidth(r, columns);
            int height = r.getHeight();
            for (int row = 0; row < height; row++) {
                int firstPixelOffset = (y + row) * columns + x;
                for (int column = 0; column < width; column++) {
                    mask.apply(
                            frame,
                            firstPixelOffset + column,
                            bigEndian,
                            planeSize
                    );
                }
            }
        });
    }

    private void validateRegions(int columns, int rows) {
        regions.forEach(r -> {
            int configuredX = r.getX();
            int configuredWidth = r.getWidth();

            int x = effectiveX(r);
            int width = effectiveWidth(r, columns);

            int y = r.getY();
            int height = r.getHeight();

            /*
             * width < 0 bedeutet:
             * x wird ignoriert und die komplette Bildbreite verwendet.
             */
            if (configuredWidth == 0
                    || y < 0
                    || height <= 0
                    || (configuredWidth > 0 && configuredX < 0)) {

                throw new IllegalArgumentException(
                        "Ungültige Region: x=" + configuredX
                                + ", y=" + y
                                + ", width=" + configuredWidth
                                + ", height=" + height
                                + ", image=" + columns + "x" + rows
                );
            }

            long right = (long) x + width;
            long bottom = (long) y + height;

            if (right > columns || bottom > rows) {
                throw new IllegalArgumentException(
                        "Region liegt ausserhalb des Bildes: x="
                                + configuredX
                                + ", y=" + y
                                + ", width=" + configuredWidth
                                + ", height=" + height
                                + ", effectiveX=" + x
                                + ", effectiveWidth=" + width
                                + ", image=" + columns + "x" + rows
                );
            }
        });
    }

    private static int requiredPositive(
            Attributes attrs,
            int tag,
            String name
    ) throws IOException {

        int value = attrs.getInt(tag, 0);

        if (value <= 0) {
            throw new IOException(
                    "Fehlendes oder ungültiges DICOM-Attribut "
                            + name + ": " + value);
        }

        return value;
    }

    @FunctionalInterface
    private interface Mask {
        void apply(
                byte[] bytes,
                int offset,
                boolean bigEndian,
                int planeSize);
    }

    private void maskByte(
            byte[] bytes,
            int offset,
            boolean bigEndian,
            int planeSize
    ) {
        bytes[offset] = (byte) pxval;
    }

    private void maskWord(
            byte[] bytes,
            int offset,
            boolean bigEndian,
            int planeSize
    ) {
        ByteUtils.shortToBytes(
                pxval,
                bytes,
                offset * 2,
                bigEndian);
    }

    private void maskColor(
            byte[] bytes,
            int offset,
            int distance
    ) {
        bytes[offset] = (byte) (pxval >> 16);
        bytes[offset + distance] = (byte) (pxval >> 8);
        bytes[offset + 2 * distance] = (byte) pxval;
    }

    private void maskColorByPixel(
            byte[] bytes,
            int offset,
            boolean bigEndian,
            int planeSize
    ) {
        maskColor(bytes, offset * 3, 1);
    }

    private void maskColorByPlane(
            byte[] bytes,
            int offset,
            boolean bigEndian,
            int planeSize
    ) {
        maskColor(bytes, offset, planeSize);
    }

    /**
     * Verhindert, dass dcm4che den von NiFi verwalteten Stream schließt.
     */
    private static final class NonClosingInputStream
            extends FilterInputStream {

        private NonClosingInputStream(InputStream input) {
            super(input);
        }

        @Override
        public void close() {
            // NiFi schließt den Stream.
        }
    }

    private static final class NonClosingOutputStream
            extends FilterOutputStream {

        private NonClosingOutputStream(OutputStream output) {
            super(output);
        }

        @Override
        public void close() throws IOException {
            flush();
            // NiFi schließt den Stream.
        }
    }

    private static int effectiveX(MaskRegion region) {
        return region.getWidth() < 0
                ? 0
                : region.getX();
    }

    private static int effectiveWidth(
            MaskRegion region,
            int imageWidth
    ) {
        return region.getWidth() < 0
                ? imageWidth
                : region.getWidth();
    }

}