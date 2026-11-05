package org.rra.dcm;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;
import org.w3c.dom.NodeList;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.metadata.IIOMetadataNode;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * JPEG Baseline (1.2.840.10008.1.2.4.50) mit dem JPEG-Codec des JDK.
 *
 * <p>Ersetzt für diesen Transfer-Syntax den opencv-Codec von dcm4che, der je
 * nach nativer Bibliothek Multiframe-JPEG nicht dekodieren kann
 * ("No implementation to handle 9 channels"). Reines Java, keine nativen
 * Abhängigkeiten. Unterstützt 8 Bit, 1 oder 3 Samples per Pixel.</p>
 *
 * <ul>
 *   <li>{@link #decode}: encapsulated JPEG Baseline nach Explicit VR Little
 *       Endian. Farbbilder werden wie bei dcm4che nach RGB gewandelt.</li>
 *   <li>{@link #encode}: native RGB/Monochrome-Daten nach JPEG Baseline,
 *       Farbe als YBR_FULL_422 (Chroma 4:2:2), ohne JFIF-Marker.</li>
 * </ul>
 */
public final class JpegBaselineCodec {

    /** Qualität für die erneute lossy-Kodierung; bewusst hoch für Bildbefunde. */
    public static final float DEFAULT_QUALITY = 0.95f;

    private static final String JPEG_METADATA_FORMAT = "javax_imageio_jpeg_image_1.0";

    private JpegBaselineCodec() {
    }

    // ------------------------------------------------------------------
    // Dekodieren
    // ------------------------------------------------------------------

    /**
     * @param customizer wird mit dem Dataset (ohne PixelData) vor dem
     *                   Schreiben aufgerufen; darf null sein
     */
    public static void decode(InputStream input, OutputStream output,
                              Consumer<Attributes> customizer) throws IOException {
        try (DicomInputStream dis = new DicomInputStream(input)) {
            dis.readFileMetaInformation();
            String ts = dis.getTransferSyntax();
            if (!UID.JPEGBaseline8Bit.equals(ts)) {
                throw new IOException("JpegBaselineCodec erwartet JPEG Baseline, nicht " + ts);
            }
            Attributes dataset = dis.readDatasetUntilPixelData();
            if (dis.tag() != Tag.PixelData || dis.length() != -1) {
                throw new IOException("Encapsulated PixelData erwartet");
            }

            int rows = dataset.getInt(Tag.Rows, 0);
            int columns = dataset.getInt(Tag.Columns, 0);
            int samples = dataset.getInt(Tag.SamplesPerPixel, 1);
            int frames = dataset.getInt(Tag.NumberOfFrames, 1);
            if (dataset.getInt(Tag.BitsAllocated, 8) != 8) {
                throw new IOException("JPEG Baseline mit BitsAllocated != 8 wird nicht unterstützt");
            }
            if (samples != 1 && samples != 3) {
                throw new IOException("SamplesPerPixel nicht unterstützt: " + samples);
            }

            byte[] offsetTable = readItem(dis);
            if (offsetTable == null) {
                throw new IOException("PixelData enthält keine Offset-Tabelle");
            }
            List<byte[]> fragments = new ArrayList<>();
            byte[] item;
            while ((item = readItem(dis)) != null) {
                fragments.add(item);
            }
            List<byte[]> frameStreams = groupFragmentsToFrames(fragments, offsetTable, frames);

            if (samples == 3) {
                dataset.setString(Tag.PhotometricInterpretation, VR.CS, "RGB");
                dataset.setInt(Tag.PlanarConfiguration, VR.US, 0);
            }
            if (customizer != null) {
                customizer.accept(dataset);
            }

            long frameSize = (long) rows * columns * samples;
            long pixelLength = frameSize * frames;
            if (pixelLength > 0xFFFFFFFEL) {
                throw new IOException("Dekodierte PixelData zu gross: " + pixelLength + " Bytes");
            }

            Attributes fmi = dataset.createFileMetaInformation(UID.ExplicitVRLittleEndian);
            DicomOutputStream dos = new DicomOutputStream(new NonClosingOutputStream(output),
                    UID.ExplicitVRLittleEndian);
            dos.writeDataset(fmi, dataset);
            dos.writeHeader(Tag.PixelData, VR.OB, (int) ((pixelLength + 1) & ~1L));

            ImageReader reader = jdkJpegReader();
            try {
                byte[] row = new byte[columns * samples];
                int[] pixels = new int[columns * samples];
                for (int f = 0; f < frames; f++) {
                    BufferedImage image = readFrame(reader, frameStreams.get(f));
                    Raster raster = image.getRaster();
                    if (raster.getWidth() != columns || raster.getHeight() != rows
                            || raster.getNumBands() != samples) {
                        throw new IOException("Frame " + f + " passt nicht zum Header: "
                                + raster.getWidth() + "x" + raster.getHeight() + "x" + raster.getNumBands()
                                + " statt " + columns + "x" + rows + "x" + samples);
                    }
                    for (int y = 0; y < rows; y++) {
                        // getPixels liefert die Bänder in Farbmodell-Reihenfolge (R,G,B)
                        raster.getPixels(0, y, columns, 1, pixels);
                        for (int i = 0; i < pixels.length; i++) {
                            row[i] = (byte) pixels[i];
                        }
                        dos.write(row);
                    }
                }
            } finally {
                reader.dispose();
            }
            if ((pixelLength & 1) != 0) {
                dos.write(0);
            }

            Attributes trailing = new Attributes(dis.bigEndian(), 4);
            dis.readAttributes(trailing, -1, -1);
            trailing.writeTo(dos);
            dos.finish();
            dos.flush();
        }
    }

    /**
     * Ordnet Fragmente den Frames zu: ein Fragment pro Frame, oder mehrere
     * Fragmente pro Frame anhand der Basic Offset Table.
     */
    static List<byte[]> groupFragmentsToFrames(List<byte[]> fragments, byte[] offsetTable, int frames)
            throws IOException {
        if (fragments.size() == frames) {
            return fragments;
        }
        if (offsetTable.length != frames * 4) {
            throw new IOException("Fragmente (" + fragments.size() + ") passen nicht zu NumberOfFrames ("
                    + frames + ") und die Basic Offset Table ist leer oder unvollständig");
        }
        long[] offsets = new long[frames];
        for (int i = 0; i < frames; i++) {
            offsets[i] = (offsetTable[4 * i] & 0xFFL)
                    | (offsetTable[4 * i + 1] & 0xFFL) << 8
                    | (offsetTable[4 * i + 2] & 0xFFL) << 16
                    | (offsetTable[4 * i + 3] & 0xFFL) << 24;
        }
        List<byte[]> result = new ArrayList<>(frames);
        ByteArrayOutputStream current = new ByteArrayOutputStream();
        long position = 0; // Offset relativ zum ersten Fragment-Item (inkl. 8 Byte Item-Header)
        int frame = 0;
        for (byte[] fragment : fragments) {
            if (frame + 1 < frames && position >= offsets[frame + 1]) {
                result.add(current.toByteArray());
                current.reset();
                frame++;
            }
            current.write(fragment, 0, fragment.length);
            position += 8 + fragment.length;
        }
        result.add(current.toByteArray());
        if (result.size() != frames) {
            throw new IOException("Fragmente konnten nicht eindeutig " + frames + " Frames zugeordnet werden");
        }
        return result;
    }

    private static BufferedImage readFrame(ImageReader reader, byte[] jpeg) throws IOException {
        try (ImageInputStream iis = ImageIO.createImageInputStream(new ByteArrayInputStream(jpeg))) {
            reader.setInput(iis, true, true);
            return reader.read(0);
        }
    }

    // ------------------------------------------------------------------
    // Kodieren
    // ------------------------------------------------------------------

    public static void encode(InputStream input, OutputStream output, float quality) throws IOException {
        try (DicomInputStream dis = new DicomInputStream(input)) {
            dis.readFileMetaInformation();
            String ts = dis.getTransferSyntax();
            if (!UID.ExplicitVRLittleEndian.equals(ts) && !UID.ImplicitVRLittleEndian.equals(ts)) {
                throw new IOException("JpegBaselineCodec.encode erwartet native Little-Endian-Daten, nicht " + ts);
            }
            Attributes dataset = dis.readDatasetUntilPixelData();
            if (dis.tag() != Tag.PixelData || dis.length() == -1) {
                throw new IOException("Native PixelData erwartet");
            }
            int rows = dataset.getInt(Tag.Rows, 0);
            int columns = dataset.getInt(Tag.Columns, 0);
            int samples = dataset.getInt(Tag.SamplesPerPixel, 1);
            int frames = dataset.getInt(Tag.NumberOfFrames, 1);
            int planar = dataset.getInt(Tag.PlanarConfiguration, 0);
            String pmi = dataset.getString(Tag.PhotometricInterpretation, "");
            if (dataset.getInt(Tag.BitsAllocated, 8) != 8 || dataset.getInt(Tag.BitsStored, 8) > 8) {
                throw new IOException("JPEG Baseline unterstützt nur 8 Bit");
            }
            if (!(samples == 3 && "RGB".equals(pmi)) && !(samples == 1 && pmi.startsWith("MONOCHROME"))) {
                throw new IOException("JPEG Baseline-Kodierung erwartet RGB oder MONOCHROME, nicht "
                        + samples + "x " + pmi);
            }

            int frameSize = Math.toIntExact((long) rows * columns * samples);
            long pixelLength = dis.unsignedLength();
            if (pixelLength < (long) frameSize * frames) {
                throw new IOException("PixelData kleiner als erwartet");
            }

            if (samples == 3) {
                dataset.setString(Tag.PhotometricInterpretation, VR.CS, "YBR_FULL_422");
                dataset.setInt(Tag.PlanarConfiguration, VR.US, 0);
            }
            Attributes fmi = dataset.createFileMetaInformation(UID.JPEGBaseline8Bit);
            DicomOutputStream dos = new DicomOutputStream(new NonClosingOutputStream(output),
                    UID.ExplicitVRLittleEndian);
            dos.writeDataset(fmi, dataset);
            dos.writeHeader(Tag.PixelData, VR.OB, -1);
            dos.writeHeader(Tag.Item, null, 0);

            ImageWriter writer = jdkJpegWriter();
            try {
                byte[] frame = new byte[frameSize];
                for (int f = 0; f < frames; f++) {
                    dis.readFully(frame);
                    BufferedImage image = toImage(frame, columns, rows, samples, planar);
                    byte[] jpeg = writeFrame(writer, image, quality);
                    dos.writeHeader(Tag.Item, null, (jpeg.length + 1) & ~1);
                    dos.write(jpeg);
                    if ((jpeg.length & 1) != 0) {
                        dos.write(0);
                    }
                }
            } finally {
                writer.dispose();
            }
            dos.writeHeader(Tag.SequenceDelimitationItem, null, 0);

            long remaining = pixelLength - (long) frameSize * frames;
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

    private static BufferedImage toImage(byte[] frame, int columns, int rows, int samples, int planar) {
        BufferedImage image = new BufferedImage(columns, rows,
                samples == 3 ? BufferedImage.TYPE_3BYTE_BGR : BufferedImage.TYPE_BYTE_GRAY);
        WritableRaster raster = image.getRaster();
        int pixels = columns * rows;
        int[] row = new int[columns * samples];
        for (int y = 0; y < rows; y++) {
            for (int x = 0; x < columns; x++) {
                int p = y * columns + x;
                for (int s = 0; s < samples; s++) {
                    int index = samples == 3 && planar == 1 ? s * pixels + p : p * samples + s;
                    row[x * samples + s] = frame[index] & 0xFF;
                }
            }
            raster.setPixels(0, y, columns, 1, row);
        }
        return image;
    }

    private static byte[] writeFrame(ImageWriter writer, BufferedImage image, float quality) throws IOException {
        ImageWriteParam param = writer.getDefaultWriteParam();
        param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
        param.setCompressionQuality(quality);

        IIOMetadata metadata = writer.getDefaultImageMetadata(
                ImageTypeSpecifier.createFromRenderedImage(image), param);
        IIOMetadataNode root = (IIOMetadataNode) metadata.getAsTree(JPEG_METADATA_FORMAT);
        // JFIF-Marker entfernen: in DICOM ist der Farbraum über die
        // Photometric Interpretation festgelegt.
        NodeList jfif = root.getElementsByTagName("app0JFIF");
        for (int i = jfif.getLength() - 1; i >= 0; i--) {
            jfif.item(i).getParentNode().removeChild(jfif.item(i));
        }
        if (image.getRaster().getNumBands() == 3) {
            // Chroma horizontal halbiert (4:2:2) = YBR_FULL_422
            NodeList components = root.getElementsByTagName("componentSpec");
            for (int i = 0; i < components.getLength(); i++) {
                IIOMetadataNode component = (IIOMetadataNode) components.item(i);
                component.setAttribute("HsamplingFactor", i == 0 ? "2" : "1");
                component.setAttribute("VsamplingFactor", "1");
            }
        }
        metadata.setFromTree(JPEG_METADATA_FORMAT, root);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ImageOutputStream ios = ImageIO.createImageOutputStream(bytes)) {
            writer.setOutput(ios);
            writer.write(null, new IIOImage(image, null, metadata), param);
        }
        return bytes.toByteArray();
    }

    // ------------------------------------------------------------------
    // Hilfsfunktionen
    // ------------------------------------------------------------------

    private static byte[] readItem(DicomInputStream dis) throws IOException {
        if (!dis.readItemHeader()) {
            return null; // Sequence Delimitation Item
        }
        byte[] value = new byte[dis.length()];
        dis.readFully(value);
        return value;
    }

    private static ImageReader jdkJpegReader() throws IOException {
        Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("jpeg");
        while (readers.hasNext()) {
            ImageReader reader = readers.next();
            if (reader.getClass().getName().equals("com.sun.imageio.plugins.jpeg.JPEGImageReader")) {
                return reader;
            }
            reader.dispose();
        }
        throw new IOException("JDK-JPEG-Reader nicht verfügbar");
    }

    private static ImageWriter jdkJpegWriter() throws IOException {
        Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpeg");
        while (writers.hasNext()) {
            ImageWriter writer = writers.next();
            if (writer.getClass().getName().equals("com.sun.imageio.plugins.jpeg.JPEGImageWriter")) {
                return writer;
            }
            writer.dispose();
        }
        throw new IOException("JDK-JPEG-Writer nicht verfügbar");
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
