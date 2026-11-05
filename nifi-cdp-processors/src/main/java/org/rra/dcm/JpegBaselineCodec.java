package org.rra.dcm;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;
import org.w3c.dom.NodeList;

import javax.imageio.IIOImage;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.metadata.IIOMetadataNode;
import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.spi.ImageWriterSpi;
import javax.imageio.stream.MemoryCacheImageInputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
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
 *   <li>Dekodieren liefert Farbbilder wie dcm4che als RGB, Planar
 *       Configuration 0.</li>
 *   <li>Kodieren erwartet RGB oder MONOCHROME und schreibt Farbe als
 *       YBR_FULL_422 (Chroma 4:2:2), ohne JFIF-Marker.</li>
 * </ul>
 *
 * <p>Reader und Writer werden direkt über ihre Service Provider erzeugt und
 * arbeiten nur im Speicher: {@code ImageIO.createImageInputStream} würde je
 * nach Einstellung über temporäre Dateien puffern und bei jedem Aufruf alle
 * registrierten Plugins (auch opencv) instanziieren.</p>
 */
public final class JpegBaselineCodec {

    /** Qualität für die erneute lossy-Kodierung; bewusst hoch für Bildbefunde. */
    public static final float DEFAULT_QUALITY = 0.95f;

    private static final String JPEG_METADATA_FORMAT = "javax_imageio_jpeg_image_1.0";
    private static final String JDK_READER = "com.sun.imageio.plugins.jpeg.JPEGImageReader";
    private static final String JDK_WRITER = "com.sun.imageio.plugins.jpeg.JPEGImageWriter";

    private static volatile ImageReaderSpi readerSpi;
    private static volatile ImageWriterSpi writerSpi;

    private JpegBaselineCodec() {
    }

    // ------------------------------------------------------------------
    // Frame-Ebene
    // ------------------------------------------------------------------

    /**
     * Dekodiert einen JPEG-Frame in {@code dest} (RGB bzw. Grau, interleaved).
     */
    public static void decodeFrame(byte[] jpeg, byte[] dest, int columns, int rows, int samples)
            throws IOException {
        ImageReader reader = readerSpi().createReaderInstance();
        try (MemoryCacheImageInputStream iis = new MemoryCacheImageInputStream(new ByteArrayInputStream(jpeg))) {
            reader.setInput(iis, true, true);
            if (reader.getWidth(0) != columns || reader.getHeight(0) != rows) {
                throw new IOException("JPEG-Frame " + reader.getWidth(0) + "x" + reader.getHeight(0)
                        + " passt nicht zum Header " + columns + "x" + rows);
            }
            BufferedImage image = new BufferedImage(columns, rows,
                    samples == 3 ? BufferedImage.TYPE_3BYTE_BGR : BufferedImage.TYPE_BYTE_GRAY);
            ImageReadParam param = reader.getDefaultReadParam();
            param.setDestination(image);
            reader.read(0, param);
            if (image.getRaster().getNumBands() != samples) {
                throw new IOException("JPEG-Frame hat " + image.getRaster().getNumBands()
                        + " Komponenten, erwartet " + samples);
            }
            byte[] data = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
            if (samples == 3) {
                for (int i = 0, n = columns * rows * 3; i < n; i += 3) {
                    dest[i] = data[i + 2];     // R
                    dest[i + 1] = data[i + 1]; // G
                    dest[i + 2] = data[i];     // B
                }
            } else {
                System.arraycopy(data, 0, dest, 0, columns * rows);
            }
        } finally {
            reader.dispose();
        }
    }

    /**
     * Kodiert einen nativen RGB- bzw. Grau-Frame als JPEG Baseline.
     */
    public static byte[] encodeFrame(byte[] frame, int columns, int rows, int samples,
                                     int planarConfiguration, float quality) throws IOException {
        BufferedImage image = new BufferedImage(columns, rows,
                samples == 3 ? BufferedImage.TYPE_3BYTE_BGR : BufferedImage.TYPE_BYTE_GRAY);
        byte[] data = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
        int pixels = columns * rows;
        if (samples == 3) {
            if (planarConfiguration == 1) {
                for (int p = 0, i = 0; p < pixels; p++, i += 3) {
                    data[i] = frame[2 * pixels + p];
                    data[i + 1] = frame[pixels + p];
                    data[i + 2] = frame[p];
                }
            } else {
                for (int i = 0, n = pixels * 3; i < n; i += 3) {
                    data[i] = frame[i + 2];
                    data[i + 1] = frame[i + 1];
                    data[i + 2] = frame[i];
                }
            }
        } else {
            System.arraycopy(frame, 0, data, 0, pixels);
        }

        ImageWriter writer = writerSpi().createWriterInstance();
        try {
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
            if (samples == 3) {
                // Chroma horizontal halbiert (4:2:2) = YBR_FULL_422
                NodeList components = root.getElementsByTagName("componentSpec");
                for (int i = 0; i < components.getLength(); i++) {
                    IIOMetadataNode component = (IIOMetadataNode) components.item(i);
                    component.setAttribute("HsamplingFactor", i == 0 ? "2" : "1");
                    component.setAttribute("VsamplingFactor", "1");
                }
            }
            metadata.setFromTree(JPEG_METADATA_FORMAT, root);

            ByteArrayOutputStream bytes = new ByteArrayOutputStream(pixels / 4);
            try (MemoryCacheImageOutputStream ios = new MemoryCacheImageOutputStream(bytes)) {
                writer.setOutput(ios);
                writer.write(null, new IIOImage(image, null, metadata), param);
            }
            return bytes.toByteArray();
        } finally {
            writer.dispose();
        }
    }

    // ------------------------------------------------------------------
    // Ganze Instanz (für Fallback-Pfad und Tests)
    // ------------------------------------------------------------------

    /**
     * Dekodiert eine JPEG-Baseline-Instanz nach Explicit VR Little Endian.
     *
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
            checkFormat(dataset.getInt(Tag.BitsAllocated, 8), samples);

            List<byte[]> frameStreams = readFrames(dis, frames);
            if (samples == 3) {
                dataset.setString(Tag.PhotometricInterpretation, VR.CS, "RGB");
                dataset.setInt(Tag.PlanarConfiguration, VR.US, 0);
            }
            if (customizer != null) {
                customizer.accept(dataset);
            }

            int frameSize = Math.toIntExact((long) rows * columns * samples);
            long pixelLength = (long) frameSize * frames;
            if (pixelLength > 0xFFFFFFFEL) {
                throw new IOException("Dekodierte PixelData zu gross: " + pixelLength + " Bytes");
            }
            DicomOutputStream dos = new DicomOutputStream(new NonClosingOutputStream(output),
                    UID.ExplicitVRLittleEndian);
            dos.writeDataset(dataset.createFileMetaInformation(UID.ExplicitVRLittleEndian), dataset);
            dos.writeHeader(Tag.PixelData, VR.OB, (int) ((pixelLength + 1) & ~1L));
            byte[] frame = new byte[frameSize];
            for (byte[] jpeg : frameStreams) {
                decodeFrame(jpeg, frame, columns, rows, samples);
                dos.write(frame);
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
     * Kodiert eine native Little-Endian-Instanz (RGB oder MONOCHROME, 8 Bit)
     * als JPEG Baseline.
     */
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
            checkEncodable(dataset);

            int frameSize = Math.toIntExact((long) rows * columns * samples);
            long pixelLength = dis.unsignedLength();
            if (pixelLength < (long) frameSize * frames) {
                throw new IOException("PixelData kleiner als erwartet");
            }
            if (samples == 3) {
                dataset.setString(Tag.PhotometricInterpretation, VR.CS, "YBR_FULL_422");
                dataset.setInt(Tag.PlanarConfiguration, VR.US, 0);
            }
            DicomOutputStream dos = new DicomOutputStream(new NonClosingOutputStream(output),
                    UID.ExplicitVRLittleEndian);
            dos.writeDataset(dataset.createFileMetaInformation(UID.JPEGBaseline8Bit), dataset);
            dos.writeHeader(Tag.PixelData, VR.OB, -1);
            dos.writeHeader(Tag.Item, null, 0);
            byte[] frame = new byte[frameSize];
            for (int f = 0; f < frames; f++) {
                dis.readFully(frame);
                writeFragment(dos, encodeFrame(frame, columns, rows, samples, planar, quality));
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

    // ------------------------------------------------------------------
    // Hilfsfunktionen
    // ------------------------------------------------------------------

    static void checkFormat(int bitsAllocated, int samples) throws IOException {
        if (bitsAllocated != 8) {
            throw new IOException("JPEG Baseline mit BitsAllocated " + bitsAllocated + " wird nicht unterstützt");
        }
        if (samples != 1 && samples != 3) {
            throw new IOException("SamplesPerPixel nicht unterstützt: " + samples);
        }
    }

    /**
     * Prüft, ob ein nativer Frame mit diesen Attributen als JPEG Baseline
     * kodiert werden kann.
     */
    static void checkEncodable(Attributes dataset) throws IOException {
        int samples = dataset.getInt(Tag.SamplesPerPixel, 1);
        String pmi = dataset.getString(Tag.PhotometricInterpretation, "");
        if (dataset.getInt(Tag.BitsAllocated, 8) != 8 || dataset.getInt(Tag.BitsStored, 8) > 8) {
            throw new IOException("JPEG Baseline unterstützt nur 8 Bit");
        }
        if (!(samples == 3 && "RGB".equals(pmi)) && !(samples == 1 && pmi.startsWith("MONOCHROME"))) {
            throw new IOException("JPEG Baseline-Kodierung erwartet RGB oder MONOCHROME, nicht "
                    + samples + "x " + pmi);
        }
    }

    /**
     * Liest alle Fragmente nach der Basic Offset Table und ordnet sie den
     * Frames zu. Der Stream steht danach hinter dem Sequence Delimitation Item.
     */
    static List<byte[]> readFrames(DicomInputStream dis, int frames) throws IOException {
        byte[] offsetTable = readItem(dis);
        if (offsetTable == null) {
            throw new IOException("PixelData enthält keine Offset-Tabelle");
        }
        List<byte[]> fragments = new ArrayList<>();
        byte[] item;
        while ((item = readItem(dis)) != null) {
            fragments.add(item);
        }
        return groupFragmentsToFrames(fragments, offsetTable, frames);
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
        long position = 0; // relativ zum ersten Fragment-Item (inkl. 8 Byte Item-Header)
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

    static byte[] readItem(DicomInputStream dis) throws IOException {
        if (!dis.readItemHeader()) {
            return null; // Sequence Delimitation Item
        }
        byte[] value = new byte[dis.length()];
        dis.readFully(value);
        return value;
    }

    static void writeFragment(DicomOutputStream dos, byte[] fragment) throws IOException {
        dos.writeHeader(Tag.Item, null, (fragment.length + 1) & ~1);
        dos.write(fragment);
        if ((fragment.length & 1) != 0) {
            dos.write(0);
        }
    }

    private static ImageReaderSpi readerSpi() throws IOException {
        ImageReaderSpi spi = readerSpi;
        if (spi == null) {
            spi = findSpi(ImageReaderSpi.class, JDK_READER);
            readerSpi = spi;
        }
        return spi;
    }

    private static ImageWriterSpi writerSpi() throws IOException {
        ImageWriterSpi spi = writerSpi;
        if (spi == null) {
            spi = findSpi(ImageWriterSpi.class, JDK_WRITER);
            writerSpi = spi;
        }
        return spi;
    }

    private static <T extends javax.imageio.spi.ImageReaderWriterSpi> T findSpi(Class<T> type, String pluginClass)
            throws IOException {
        Iterator<T> providers = IIORegistry.getDefaultInstance().getServiceProviders(type, true);
        while (providers.hasNext()) {
            T spi = providers.next();
            if (pluginClass.equals(spi.getPluginClassName())) {
                return spi;
            }
        }
        throw new IOException("JDK-JPEG-Codec nicht verfügbar: " + pluginClass);
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
