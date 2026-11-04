package org.rra.dcm;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Kodiert eine native (Explicit VR Little Endian) DICOM-Instanz als
 * RLE Lossless (1.2.840.10008.1.2.5) gemäss DICOM PS3.5 Annex G.
 *
 * <p>dcm4che bringt nur einen RLE-Decoder mit; dieser Encoder schliesst die
 * Lücke. Unterstützt werden 8 und 16 Bit sowie 1 oder 3 Samples per Pixel,
 * Planar Configuration 0 und 1 im Eingang.</p>
 *
 * <ul>
 *   <li>Pro Frame ein Fragment, leere Basic Offset Table.</li>
 *   <li>Pro Sample und Byte ein Segment, höchstwertiges Byte zuerst (G.2).</li>
 *   <li>PackBits-Kodierung zeilenweise, Läufe überschreiten keine Zeile (G.3.1).</li>
 *   <li>Segmente auf gerade Länge aufgefüllt.</li>
 * </ul>
 */
public final class RleLosslessEncoder {

    private static final int MAX_SEGMENTS = 15;
    private static final int HEADER_LENGTH = 64;

    private RleLosslessEncoder() {
    }

    /**
     * @param input  native DICOM-Instanz inkl. File Meta Information
     * @param output Ziel; wird nicht geschlossen
     */
    public static void encode(InputStream input, OutputStream output) throws IOException {
        try (DicomInputStream dis = new DicomInputStream(input)) {
            dis.readFileMetaInformation();
            String sourceTs = dis.getTransferSyntax();
            if (!UID.ExplicitVRLittleEndian.equals(sourceTs)
                    && !UID.ImplicitVRLittleEndian.equals(sourceTs)) {
                throw new IOException("RLE-Encoder erwartet Little-Endian-Pixeldaten, nicht " + sourceTs);
            }
            Attributes dataset = dis.readDatasetUntilPixelData();
            if (dis.tag() != Tag.PixelData) {
                throw new IOException("DICOM-Instanz enthält kein PixelData-Element");
            }
            if (dis.length() == -1) {
                throw new IOException("RLE-Encoder erwartet native PixelData, nicht encapsulated");
            }

            int rows = dataset.getInt(Tag.Rows, 0);
            int columns = dataset.getInt(Tag.Columns, 0);
            int samples = dataset.getInt(Tag.SamplesPerPixel, 1);
            int bitsAllocated = dataset.getInt(Tag.BitsAllocated, 0);
            int frames = dataset.getInt(Tag.NumberOfFrames, 1);
            int planarConfiguration = dataset.getInt(Tag.PlanarConfiguration, 0);
            if (rows <= 0 || columns <= 0 || frames <= 0) {
                throw new IOException("Ungültige Bildgeometrie: " + rows + "x" + columns + ", Frames=" + frames);
            }
            if (bitsAllocated != 8 && bitsAllocated != 16) {
                throw new IOException("RLE-Encoder unterstützt nur 8 oder 16 BitsAllocated, nicht " + bitsAllocated);
            }
            if (samples != 1 && samples != 3) {
                throw new IOException("RLE-Encoder unterstützt nur 1 oder 3 SamplesPerPixel, nicht " + samples);
            }
            int bytesPerSample = bitsAllocated / 8;
            int segmentCount = samples * bytesPerSample;
            if (segmentCount > MAX_SEGMENTS) {
                throw new IOException("Zu viele RLE-Segmente: " + segmentCount);
            }

            long frameSizeLong = (long) rows * columns * samples * bytesPerSample;
            if (frameSizeLong > Integer.MAX_VALUE) {
                throw new IOException("Frame zu gross für RLE-Kodierung: " + frameSizeLong + " Bytes");
            }
            int frameSize = (int) frameSizeLong;
            long pixelDataLength = dis.unsignedLength();
            if (pixelDataLength < frameSizeLong * frames) {
                throw new IOException("PixelData kleiner als erwartet: vorhanden=" + pixelDataLength
                        + ", erwartet=" + frameSizeLong * frames);
            }

            // Bei RLE ist die Anordnung der Komponenten durch die Segmente
            // festgelegt; Planar Configuration wird auf 0 gesetzt.
            if (samples > 1) {
                dataset.setInt(Tag.PlanarConfiguration, VR.US, 0);
            }
            dataset.remove(Tag.PixelData);

            Attributes fmi = dataset.createFileMetaInformation(UID.RLELossless);
            DicomOutputStream dos = new DicomOutputStream(new NonClosingOutputStream(output),
                    UID.ExplicitVRLittleEndian);
            dos.writeDataset(fmi, dataset);
            dos.writeHeader(Tag.PixelData, VR.OB, -1);
            dos.writeHeader(Tag.Item, null, 0); // leere Basic Offset Table

            byte[] frame = new byte[frameSize];
            byte[] segment = new byte[rows * columns];
            ByteArrayOutputStream fragment = new ByteArrayOutputStream(frameSize / 2 + HEADER_LENGTH);
            ByteArrayOutputStream encodedSegment = new ByteArrayOutputStream(rows * columns / 2 + 16);
            for (int f = 0; f < frames; f++) {
                dis.readFully(frame);
                encodeFrame(frame, rows, columns, samples, bytesPerSample, planarConfiguration,
                        segment, encodedSegment, fragment);
                dos.writeHeader(Tag.Item, null, fragment.size());
                fragment.writeTo(dos);
            }
            dos.writeHeader(Tag.SequenceDelimitationItem, null, 0);

            // Eventuelles Padding im nativen PixelData überspringen und
            // Attribute nach PixelData (z.B. Trailing Padding) übernehmen.
            long remaining = pixelDataLength - frameSizeLong * frames;
            if (remaining > 0) {
                dis.skipFully(remaining);
            }
            Attributes trailing = new Attributes(dis.bigEndian(), 4);
            dis.readAttributes(trailing, -1, -1);
            trailing.writeTo(dos);

            dos.finish();
            dos.flush();
        }
    }

    static void encodeFrame(byte[] frame, int rows, int columns, int samples, int bytesPerSample,
                            int planarConfiguration, byte[] segment,
                            ByteArrayOutputStream encodedSegment, ByteArrayOutputStream fragment) {
        int pixels = rows * columns;
        int segmentCount = samples * bytesPerSample;
        byte[][] encodedSegments = new byte[segmentCount][];

        int segmentIndex = 0;
        for (int sample = 0; sample < samples; sample++) {
            // Höchstwertiges Byte zuerst: bei Little Endian ist das der
            // letzte Byte-Index innerhalb eines Samples.
            for (int byteIndex = bytesPerSample - 1; byteIndex >= 0; byteIndex--) {
                extractSegment(frame, pixels, samples, bytesPerSample, planarConfiguration,
                        sample, byteIndex, segment);
                encodedSegment.reset();
                for (int row = 0; row < rows; row++) {
                    packBits(segment, row * columns, columns, encodedSegment);
                }
                if ((encodedSegment.size() & 1) != 0) {
                    encodedSegment.write(0); // auf gerade Länge auffüllen
                }
                encodedSegments[segmentIndex++] = encodedSegment.toByteArray();
            }
        }

        fragment.reset();
        byte[] header = new byte[HEADER_LENGTH];
        writeIntLE(header, 0, segmentCount);
        int offset = HEADER_LENGTH;
        for (int i = 0; i < segmentCount; i++) {
            writeIntLE(header, 4 + 4 * i, offset);
            offset += encodedSegments[i].length;
        }
        fragment.write(header, 0, header.length);
        for (byte[] encoded : encodedSegments) {
            fragment.write(encoded, 0, encoded.length);
        }
    }

    private static void extractSegment(byte[] frame, int pixels, int samples, int bytesPerSample,
                                       int planarConfiguration, int sample, int byteIndex,
                                       byte[] segment) {
        if (samples > 1 && planarConfiguration == 1) {
            int planeStart = sample * pixels * bytesPerSample;
            for (int p = 0; p < pixels; p++) {
                segment[p] = frame[planeStart + p * bytesPerSample + byteIndex];
            }
        } else {
            int stride = samples * bytesPerSample;
            int start = sample * bytesPerSample + byteIndex;
            for (int p = 0; p < pixels; p++) {
                segment[p] = frame[start + p * stride];
            }
        }
    }

    /**
     * PackBits nach PS3.5 G.3.1: Wiederholung von 2..128 Bytes als
     * (1 - n, Byte), Literal von 1..128 Bytes als (n - 1, Bytes...).
     * Der Wert -128 wird nicht erzeugt.
     */
    static void packBits(byte[] data, int start, int length, ByteArrayOutputStream out) {
        int end = start + length;
        int i = start;
        while (i < end) {
            int run = 1;
            while (i + run < end && run < 128 && data[i + run] == data[i]) {
                run++;
            }
            if (run >= 2) {
                out.write(1 - run);
                out.write(data[i]);
                i += run;
                continue;
            }
            int literalStart = i;
            int literal = 0;
            while (i < end && literal < 128) {
                if (i + 1 < end && data[i] == data[i + 1]) {
                    break; // ab hier lohnt sich ein Lauf
                }
                i++;
                literal++;
            }
            if (literal == 0) {
                continue;
            }
            out.write(literal - 1);
            out.write(data, literalStart, literal);
        }
    }

    private static void writeIntLE(byte[] buffer, int offset, int value) {
        buffer[offset] = (byte) value;
        buffer[offset + 1] = (byte) (value >>> 8);
        buffer[offset + 2] = (byte) (value >>> 16);
        buffer[offset + 3] = (byte) (value >>> 24);
    }

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
