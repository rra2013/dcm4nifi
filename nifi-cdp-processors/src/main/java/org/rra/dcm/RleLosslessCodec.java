package org.rra.dcm;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * RLE Lossless (1.2.840.10008.1.2.5) gemäss DICOM PS3.5 Annex G.
 *
 * <p>dcm4che bringt nur einen RLE-Decoder über ImageIO mit; dieser Codec
 * kodiert und dekodiert Frames direkt auf Byte-Arrays, ohne ImageIO und ohne
 * temporäre Dateien. Unterstützt werden 8 und 16 Bit sowie 1 oder 3 Samples
 * per Pixel.</p>
 *
 * <ul>
 *   <li>Ein Fragment pro Frame, ein Segment pro Sample und Byte,
 *       höchstwertiges Byte zuerst (G.2).</li>
 *   <li>PackBits zeilenweise, Läufe überschreiten keine Zeile (G.3.1).</li>
 *   <li>Segmente auf gerade Länge aufgefüllt.</li>
 *   <li>Native Frames sind Little Endian; Planar Configuration 0 und 1 beim
 *       Kodieren, Dekodieren liefert immer Planar Configuration 0.</li>
 * </ul>
 */
public final class RleLosslessCodec {

    private static final int MAX_SEGMENTS = 15;
    private static final int HEADER_LENGTH = 64;

    private RleLosslessCodec() {
    }

    // ------------------------------------------------------------------
    // Frame-Ebene
    // ------------------------------------------------------------------

    /**
     * Kodiert einen nativen Frame (Little Endian) als RLE-Fragment.
     */
    public static byte[] encodeFrame(byte[] frame, int rows, int columns, int samples,
                                     int bytesPerSample, int planarConfiguration) {
        int pixels = rows * columns;
        int segmentCount = checkSegments(samples, bytesPerSample);

        // Worst Case je Zeile: nur Literale, je 128 Bytes ein Steuerbyte.
        int maxSegmentLength = rows * (columns + (columns + 127) / 128) + 1;
        byte[] out = new byte[HEADER_LENGTH + segmentCount * maxSegmentLength];
        byte[] segment = new byte[pixels];

        writeIntLE(out, 0, segmentCount);
        int position = HEADER_LENGTH;
        int segmentIndex = 0;
        for (int sample = 0; sample < samples; sample++) {
            for (int byteIndex = bytesPerSample - 1; byteIndex >= 0; byteIndex--) {
                extractSegment(frame, pixels, samples, bytesPerSample, planarConfiguration,
                        sample, byteIndex, segment);
                writeIntLE(out, 4 + 4 * segmentIndex++, position);
                for (int row = 0; row < rows; row++) {
                    position = packBits(segment, row * columns, columns, out, position);
                }
                if ((position & 1) != 0) {
                    out[position++] = 0;
                }
            }
        }
        return Arrays.copyOf(out, position);
    }

    /**
     * Dekodiert ein RLE-Fragment in einen nativen Frame (Little Endian,
     * Planar Configuration 0).
     */
    public static void decodeFrame(byte[] fragment, byte[] dest, int rows, int columns,
                                   int samples, int bytesPerSample) throws IOException {
        int pixels = rows * columns;
        int segmentCount = checkSegments(samples, bytesPerSample);
        if (fragment.length < HEADER_LENGTH) {
            throw new IOException("RLE-Fragment kürzer als der RLE-Header");
        }
        int declared = readIntLE(fragment, 0);
        if (declared != segmentCount) {
            throw new IOException("RLE-Fragment enthält " + declared + " Segmente, erwartet "
                    + segmentCount);
        }
        int stride = samples * bytesPerSample;
        for (int k = 0; k < segmentCount; k++) {
            int start = readIntLE(fragment, 4 + 4 * k);
            int end = k + 1 < segmentCount ? readIntLE(fragment, 8 + 4 * k) : fragment.length;
            if (start < HEADER_LENGTH || end > fragment.length || start > end) {
                throw new IOException("Ungültiger RLE-Segment-Offset");
            }
            int sample = k / bytesPerSample;
            int byteIndex = bytesPerSample - 1 - (k % bytesPerSample);
            int out = sample * bytesPerSample + byteIndex;

            int pixel = 0;
            int i = start;
            while (pixel < pixels && i < end) {
                int control = fragment[i++];
                if (control >= 0) {
                    int count = Math.min(control + 1, Math.min(pixels - pixel, end - i));
                    for (int c = 0; c < count; c++) {
                        dest[out + (pixel++) * stride] = fragment[i++];
                    }
                } else if (control != -128 && i < end) {
                    int count = Math.min(1 - control, pixels - pixel);
                    byte value = fragment[i++];
                    for (int c = 0; c < count; c++) {
                        dest[out + (pixel++) * stride] = value;
                    }
                }
            }
            if (pixel < pixels) {
                throw new IOException("RLE-Segment " + k + " zu kurz: " + pixel + " von " + pixels + " Pixeln");
            }
        }
    }

    // ------------------------------------------------------------------
    // Ganze Instanz (für den Fallback-Pfad)
    // ------------------------------------------------------------------

    /**
     * Kodiert eine native Little-Endian-Instanz als RLE Lossless.
     *
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
            if (dis.tag() != Tag.PixelData || dis.length() == -1) {
                throw new IOException("RLE-Encoder erwartet native PixelData");
            }
            int rows = dataset.getInt(Tag.Rows, 0);
            int columns = dataset.getInt(Tag.Columns, 0);
            int samples = dataset.getInt(Tag.SamplesPerPixel, 1);
            int bitsAllocated = dataset.getInt(Tag.BitsAllocated, 0);
            int frames = dataset.getInt(Tag.NumberOfFrames, 1);
            int planarConfiguration = dataset.getInt(Tag.PlanarConfiguration, 0);
            if (bitsAllocated != 8 && bitsAllocated != 16) {
                throw new IOException("RLE-Encoder unterstützt nur 8 oder 16 BitsAllocated, nicht " + bitsAllocated);
            }
            int bytesPerSample = bitsAllocated / 8;
            long frameSizeLong = (long) rows * columns * samples * bytesPerSample;
            if (rows <= 0 || columns <= 0 || frames <= 0 || frameSizeLong > Integer.MAX_VALUE) {
                throw new IOException("Ungültige Bildgeometrie: " + rows + "x" + columns + ", Frames=" + frames);
            }
            long pixelDataLength = dis.unsignedLength();
            if (pixelDataLength < frameSizeLong * frames) {
                throw new IOException("PixelData kleiner als erwartet");
            }

            if (samples > 1) {
                dataset.setInt(Tag.PlanarConfiguration, VR.US, 0);
            }
            DicomOutputStream dos = new DicomOutputStream(new NonClosingOutputStream(output),
                    UID.ExplicitVRLittleEndian);
            dos.writeDataset(dataset.createFileMetaInformation(UID.RLELossless), dataset);
            dos.writeHeader(Tag.PixelData, VR.OB, -1);
            dos.writeHeader(Tag.Item, null, 0);
            byte[] frame = new byte[(int) frameSizeLong];
            for (int f = 0; f < frames; f++) {
                dis.readFully(frame);
                byte[] fragment = encodeFrame(frame, rows, columns, samples, bytesPerSample,
                        planarConfiguration);
                dos.writeHeader(Tag.Item, null, fragment.length);
                dos.write(fragment);
            }
            dos.writeHeader(Tag.SequenceDelimitationItem, null, 0);

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

    // ------------------------------------------------------------------
    // Hilfsfunktionen
    // ------------------------------------------------------------------

    private static int checkSegments(int samples, int bytesPerSample) {
        if (samples != 1 && samples != 3) {
            throw new IllegalArgumentException("RLE unterstützt hier nur 1 oder 3 SamplesPerPixel, nicht " + samples);
        }
        if (bytesPerSample != 1 && bytesPerSample != 2) {
            throw new IllegalArgumentException("RLE unterstützt hier nur 8 oder 16 Bit");
        }
        int segments = samples * bytesPerSample;
        if (segments > MAX_SEGMENTS) {
            throw new IllegalArgumentException("Zu viele RLE-Segmente: " + segments);
        }
        return segments;
    }

    private static void extractSegment(byte[] frame, int pixels, int samples, int bytesPerSample,
                                       int planarConfiguration, int sample, int byteIndex,
                                       byte[] segment) {
        if (samples > 1 && planarConfiguration == 1) {
            int start = sample * pixels * bytesPerSample + byteIndex;
            if (bytesPerSample == 1) {
                System.arraycopy(frame, start, segment, 0, pixels);
            } else {
                for (int p = 0; p < pixels; p++) {
                    segment[p] = frame[start + p * bytesPerSample];
                }
            }
        } else {
            int stride = samples * bytesPerSample;
            int start = sample * bytesPerSample + byteIndex;
            if (stride == 1) {
                System.arraycopy(frame, 0, segment, 0, pixels);
            } else {
                for (int p = 0, i = start; p < pixels; p++, i += stride) {
                    segment[p] = frame[i];
                }
            }
        }
    }

    /**
     * PackBits nach PS3.5 G.3.1: Wiederholung von 2..128 Bytes als
     * (1 - n, Byte), Literal von 1..128 Bytes als (n - 1, Bytes...).
     * Der Wert -128 wird nicht erzeugt.
     *
     * @return neue Schreibposition in {@code out}
     */
    static int packBits(byte[] data, int start, int length, byte[] out, int position) {
        int end = start + length;
        int i = start;
        while (i < end) {
            byte value = data[i];
            int run = 1;
            while (i + run < end && run < 128 && data[i + run] == value) {
                run++;
            }
            if (run >= 2) {
                out[position++] = (byte) (1 - run);
                out[position++] = value;
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
            out[position++] = (byte) (literal - 1);
            System.arraycopy(data, literalStart, out, position, literal);
            position += literal;
        }
        return position;
    }

    private static int readIntLE(byte[] buffer, int offset) {
        return (buffer[offset] & 0xFF)
                | (buffer[offset + 1] & 0xFF) << 8
                | (buffer[offset + 2] & 0xFF) << 16
                | (buffer[offset + 3] & 0xFF) << 24;
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
