package org.rra.processors;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Sequence;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.util.UIDUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rra.dcm.Dicom2DicomTranscoder;
import org.rra.dcm.JpegBaselineCodec;
import org.rra.dcm.MaskMultiFrameUSTransformer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class MaskMultiFrameUSTest {

    private static final int ROWS = 2;
    private static final int COLUMNS = 3;
    private static final int NUMBER_OF_FRAMES = 2;
    private static final Path REAL_US_TEST_DIRECTORY = Path.of(
            System.getProperty("dicom.us.test.dir", "/mnt/f/DICOM/US"));

    private TestRunner testRunner;

    @BeforeEach
    void init() {
        testRunner = TestRunners.newTestRunner(MaskMultiFrameUS.class);
        testRunner.setProperty(MaskMultiFrameUS.REGIONS, "[1,0,1,1]");
        testRunner.setProperty(MaskMultiFrameUS.COLOR, "0");
    }

    @Test
    void masksConfiguredPixelsInEveryUltrasoundFrame() throws Exception {
        byte[] originalPixels = new byte[]{10, 11, 12, 13, 14, 15,
                20, 21, 22, 23, 24, 25};

        testRunner.enqueue(createDicom(
                MaskMultiFrameUSTransformer.ULTRASOUND_MULTI_FRAME_SOP_CLASS_UID,
                NUMBER_OF_FRAMES, 1, "MONOCHROME2", originalPixels));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(MaskMultiFrameUS.REL_SUCCESS, 1);
        MockFlowFile result = testRunner.getFlowFilesForRelationship(
                MaskMultiFrameUS.REL_SUCCESS).get(0);
        result.assertAttributeEquals(MaskMultiFrameUS.ATTR_SOURCE_TS, UID.ExplicitVRLittleEndian);
        result.assertAttributeEquals(MaskMultiFrameUS.ATTR_OUTPUT_TS, UID.ExplicitVRLittleEndian);

        Attributes[] parsed = readFully(result.toByteArray());
        assertEquals(UID.ExplicitVRLittleEndian, parsed[0].getString(Tag.TransferSyntaxUID));
        Attributes dataset = parsed[1];
        assertEquals(MaskMultiFrameUSTransformer.ULTRASOUND_MULTI_FRAME_SOP_CLASS_UID,
                dataset.getString(Tag.SOPClassUID));
        assertEquals(NUMBER_OF_FRAMES, dataset.getInt(Tag.NumberOfFrames, 0));
        assertArrayEquals(new byte[]{10, 0, 12, 13, 14, 15,
                20, 0, 22, 23, 24, 25}, dataset.getBytes(Tag.PixelData));
        // Lossless Eingang ohne Historie: keine Lossy-Kennzeichnung erfinden
        assertEquals(null, dataset.getString(Tag.LossyImageCompression));
    }

    @Test
    void convertsRgbMaskValueForYbrFullPixelData() throws Exception {
        byte[] pixels = new byte[ROWS * COLUMNS * 3 * NUMBER_OF_FRAMES];
        for (int i = 0; i < pixels.length; i += 3) {
            pixels[i] = (byte) 180;      // Y
            pixels[i + 1] = (byte) 128;  // Cb
            pixels[i + 2] = (byte) 128;  // Cr
        }
        testRunner.enqueue(createDicom(
                MaskMultiFrameUSTransformer.ULTRASOUND_MULTI_FRAME_SOP_CLASS_UID,
                NUMBER_OF_FRAMES, 3, "YBR_FULL", pixels));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(MaskMultiFrameUS.REL_SUCCESS, 1);
        Attributes dataset = readFully(testRunner.getFlowFilesForRelationship(
                MaskMultiFrameUS.REL_SUCCESS).get(0).toByteArray())[1];
        assertEquals("YBR_FULL", dataset.getString(Tag.PhotometricInterpretation));

        byte[] masked = dataset.getBytes(Tag.PixelData);
        int frameSize = ROWS * COLUMNS * 3;
        for (int frame = 0; frame < NUMBER_OF_FRAMES; frame++) {
            int offset = frame * frameSize + 3; // Pixel (x=1, y=0)
            // RGB-Schwarz entspricht YBR_FULL (0,128,128), nicht (0,0,0)
            assertEquals(0, masked[offset] & 0xFF);
            assertEquals(128, masked[offset + 1] & 0xFF);
            assertEquals(128, masked[offset + 2] & 0xFF);
            // Nachbarpixel unverändert
            assertEquals(180, masked[frame * frameSize] & 0xFF);
        }
    }

    @Test
    void evaluatesPixelValueExpressionPerFlowFile() throws Exception {
        testRunner.setProperty(MaskMultiFrameUS.COLOR, "${mask.value}");
        byte[] pixels = new byte[]{10, 11, 12, 13, 14, 15, 20, 21, 22, 23, 24, 25};
        testRunner.enqueue(createDicom(
                MaskMultiFrameUSTransformer.ULTRASOUND_MULTI_FRAME_SOP_CLASS_UID,
                NUMBER_OF_FRAMES, 1, "MONOCHROME2", pixels), Map.of("mask.value", "0x7F"));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(MaskMultiFrameUS.REL_SUCCESS, 1);
        byte[] masked = readFully(testRunner.getFlowFilesForRelationship(
                MaskMultiFrameUS.REL_SUCCESS).get(0).toByteArray())[1].getBytes(Tag.PixelData);
        assertEquals(0x7F, masked[1] & 0xFF);
        assertEquals(0x7F, masked[7] & 0xFF);
    }

    @Test
    void masksAllFramesWithParallelFrameThreads() throws Exception {
        testRunner.setProperty(MaskMultiFrameUS.FRAME_THREADS, "3");
        int frames = 9;
        byte[] pixels = new byte[ROWS * COLUMNS * frames];
        Arrays.fill(pixels, (byte) 50);
        testRunner.enqueue(createDicom(
                MaskMultiFrameUSTransformer.ULTRASOUND_MULTI_FRAME_SOP_CLASS_UID,
                frames, 1, "MONOCHROME2", pixels));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(MaskMultiFrameUS.REL_SUCCESS, 1);
        byte[] masked = readFully(testRunner.getFlowFilesForRelationship(
                MaskMultiFrameUS.REL_SUCCESS).get(0).toByteArray())[1].getBytes(Tag.PixelData);
        for (int frame = 0; frame < frames; frame++) {
            int offset = frame * ROWS * COLUMNS;
            assertEquals(50, masked[offset]);
            assertEquals(0, masked[offset + 1], "Frame " + frame + " nicht maskiert");
            assertEquals(50, masked[offset + 2]);
        }
    }

    @Test
    void marksCleanPixelDataWhenEnabled() throws Exception {
        testRunner.setProperty(MaskMultiFrameUS.MARK_CLEAN_PIXEL_DATA, "true");
        testRunner.enqueue(createDicom(
                MaskMultiFrameUSTransformer.ULTRASOUND_MULTI_FRAME_SOP_CLASS_UID,
                NUMBER_OF_FRAMES, 1, "MONOCHROME2", new byte[ROWS * COLUMNS * NUMBER_OF_FRAMES]));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(MaskMultiFrameUS.REL_SUCCESS, 1);
        Attributes dataset = readFully(testRunner.getFlowFilesForRelationship(
                MaskMultiFrameUS.REL_SUCCESS).get(0).toByteArray())[1];
        assertEquals("NO", dataset.getString(Tag.BurnedInAnnotation));
        Sequence methods = dataset.getSequence(Tag.DeidentificationMethodCodeSequence);
        assertNotNull(methods);
        assertTrue(methods.stream().anyMatch(item ->
                "113101".equals(item.getString(Tag.CodeValue))
                        && "DCM".equals(item.getString(Tag.CodingSchemeDesignator))));
    }

    @Test
    void routesUltrasoundSingleFrameToUnsupported() throws Exception {
        testRunner.enqueue(createDicom(
                MaskMultiFrameUSTransformer.ULTRASOUND_MULTI_FRAME_SOP_CLASS_UID,
                1, 1, "MONOCHROME2", new byte[]{1, 2, 3, 4, 5, 6}));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(MaskMultiFrameUS.REL_UNSUPPORTED, 1);
        MockFlowFile unsupported = testRunner.getFlowFilesForRelationship(
                MaskMultiFrameUS.REL_UNSUPPORTED).get(0);
        unsupported.assertAttributeExists(MaskMultiFrameUS.ATTR_UNSUPPORTED);
        unsupported.assertAttributeNotExists(MaskMultiFrameUS.ATTR_ERROR);
        assertTrue(testRunner.getLogger().getErrorMessages().isEmpty(), "kein Error-Log erwartet");
    }

    @Test
    void routesOtherSopClassesToUnsupportedUnchanged() throws Exception {
        byte[] content = createDicom(
                UID.UltrasoundImageStorage,
                NUMBER_OF_FRAMES, 1, "MONOCHROME2",
                new byte[ROWS * COLUMNS * NUMBER_OF_FRAMES]);
        testRunner.enqueue(content);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(MaskMultiFrameUS.REL_UNSUPPORTED, 1);
        MockFlowFile unsupported = testRunner.getFlowFilesForRelationship(
                MaskMultiFrameUS.REL_UNSUPPORTED).get(0);
        unsupported.assertContentEquals(content);
        assertTrue(unsupported.getAttribute(MaskMultiFrameUS.ATTR_UNSUPPORTED)
                .contains(UID.UltrasoundImageStorage));
        assertTrue(testRunner.getLogger().getErrorMessages().isEmpty(), "kein Error-Log erwartet");
    }

    @Test
    void routesNonDicomContentToFailure() {
        testRunner.enqueue("kein DICOM".getBytes());
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(MaskMultiFrameUS.REL_FAILURE, 1);
        testRunner.getFlowFilesForRelationship(MaskMultiFrameUS.REL_FAILURE).get(0)
                .assertAttributeExists(MaskMultiFrameUS.ATTR_ERROR);
    }

    // ------------------------------------------------------------------
    // Tests mit echten Dateien (werden übersprungen, wenn nicht vorhanden)
    // ------------------------------------------------------------------

    @Test
    void writesLossyJpegBaselineInputAsRleLosslessByDefault() throws Exception {
        Attributes dataset = processRealMultiFrame("dicom.us.test.jpeg.file",
                UID.JPEGBaseline8Bit, UID.RLELossless);
        assertEquals("01", dataset.getString(Tag.LossyImageCompression));
        String[] methods = dataset.getStrings(Tag.LossyImageCompressionMethod);
        assertNotNull(methods);
        assertEquals("ISO_10918_1", methods[0]);
        assertTrue(dataset.getStrings(Tag.LossyImageCompressionRatio) != null
                && dataset.getStrings(Tag.LossyImageCompressionRatio).length >= 1);
        assertTrue(!dataset.getString(Tag.PhotometricInterpretation, "").startsWith("YBR_FULL_422"),
                "YBR_FULL_422 ist für RLE Lossless nicht zulässig");
    }

    @Test
    void reencodesLossyJpegBaselineInputWhenOriginalIsConfigured() throws Exception {
        testRunner.setProperty(MaskMultiFrameUS.LOSSY_OUTPUT, MaskMultiFrameUS.LOSSY_OUTPUT_ORIGINAL);
        Attributes dataset = processRealMultiFrame("dicom.us.test.jpeg.file",
                UID.JPEGBaseline8Bit, UID.JPEGBaseline8Bit);
        assertEquals("01", dataset.getString(Tag.LossyImageCompression));
        String[] methods = dataset.getStrings(Tag.LossyImageCompressionMethod);
        String[] ratios = dataset.getStrings(Tag.LossyImageCompressionRatio);
        assertNotNull(methods);
        assertNotNull(ratios);
        assertTrue(methods.length >= 2, "Neuer lossy Schritt muss angehängt sein");
        assertEquals(methods.length, ratios.length);
    }

    @Test
    void keepsRleLosslessInputAsRleLossless() throws Exception {
        processRealMultiFrame("dicom.us.test.rle.file", UID.RLELossless, UID.RLELossless);
    }

    private Attributes processRealMultiFrame(String fileProperty, String sourceTransferSyntax,
                                             String expectedOutputTransferSyntax) throws Exception {
        Path source = locateRealMultiFrame(fileProperty, sourceTransferSyntax);

        // 16x16 deckt eine komplette JPEG-MCU ab, damit Pixel (0,0) auch nach
        // erneuter lossy-Kodierung nahe am Maskwert liegt.
        testRunner.setProperty(MaskMultiFrameUS.REGIONS, "[0,0,16,16]");
        testRunner.enqueue(Files.readAllBytes(source));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(MaskMultiFrameUS.REL_SUCCESS, 1);
        MockFlowFile result = testRunner.getFlowFilesForRelationship(
                MaskMultiFrameUS.REL_SUCCESS).get(0);
        result.assertAttributeEquals(MaskMultiFrameUS.ATTR_SOURCE_TS, sourceTransferSyntax);
        result.assertAttributeEquals(MaskMultiFrameUS.ATTR_OUTPUT_TS, expectedOutputTransferSyntax);

        Attributes originalHeader = readHeader(Files.readAllBytes(source))[1];
        Attributes[] header = readHeader(result.toByteArray());
        assertEquals(expectedOutputTransferSyntax, header[0].getString(Tag.TransferSyntaxUID));
        Attributes dataset = header[1];
        assertEquals(MaskMultiFrameUSTransformer.ULTRASOUND_MULTI_FRAME_SOP_CLASS_UID,
                dataset.getString(Tag.SOPClassUID));
        assertEquals(originalHeader.getInt(Tag.NumberOfFrames, 1), dataset.getInt(Tag.NumberOfFrames, 0));
        assertEquals(originalHeader.getInt(Tag.Rows, 0), dataset.getInt(Tag.Rows, -1));
        assertEquals(originalHeader.getInt(Tag.Columns, 0), dataset.getInt(Tag.Columns, -1));

        assertMaskedOriginPixelInFirstAndLastFrame(result.toByteArray(),
                UID.JPEGBaseline8Bit.equals(expectedOutputTransferSyntax) ? 8 : 0);
        return dataset;
    }

    /**
     * Dekodiert das Ergebnis und prüft Pixel (0,0) im ersten und letzten Frame.
     * Erwartet wird Schwarz im jeweiligen Farbraum; bei lossy Ausgabe mit Toleranz.
     */
    private static void assertMaskedOriginPixelInFirstAndLastFrame(byte[] dicom, int tolerance)
            throws IOException {
        ByteArrayOutputStream decoded = new ByteArrayOutputStream();
        String outputTs = readHeader(dicom)[0].getString(Tag.TransferSyntaxUID);
        if (UID.JPEGBaseline8Bit.equals(outputTs)) {
            // nicht über opencv, siehe JpegBaselineCodec
            JpegBaselineCodec.decode(new ByteArrayInputStream(dicom), decoded, null);
        } else {
            Dicom2DicomTranscoder.transcode(new ByteArrayInputStream(dicom), decoded,
                    UID.ExplicitVRLittleEndian);
        }
        Attributes dataset = readFully(decoded.toByteArray())[1];

        int rows = dataset.getInt(Tag.Rows, 0);
        int columns = dataset.getInt(Tag.Columns, 0);
        int samples = dataset.getInt(Tag.SamplesPerPixel, 1);
        int frames = dataset.getInt(Tag.NumberOfFrames, 1);
        int bytesPerSample = (dataset.getInt(Tag.BitsAllocated, 8) + 7) / 8;
        boolean planar = samples == 3 && dataset.getInt(Tag.PlanarConfiguration, 0) == 1;
        String pmi = dataset.getString(Tag.PhotometricInterpretation, "");
        int[] expected = samples == 3 && pmi.startsWith("YBR")
                ? new int[]{0, 128, 128}
                : new int[]{0, 0, 0};

        byte[] pixels = dataset.getBytes(Tag.PixelData);
        int planeSize = rows * columns * bytesPerSample;
        int frameSize = planeSize * samples;
        for (int frame : new int[]{0, frames - 1}) {
            for (int sample = 0; sample < samples; sample++) {
                int offset = frame * frameSize + (planar ? sample * planeSize : sample * bytesPerSample);
                int value = pixels[offset] & 0xFF;
                assertTrue(Math.abs(value - expected[sample]) <= tolerance,
                        "Frame " + frame + ", Sample " + sample + " (" + pmi + "): erwartet "
                                + expected[sample] + ", gefunden " + value);
            }
        }
    }

    private static Path locateRealMultiFrame(String fileProperty, String transferSyntax)
            throws IOException {
        SampleSearch search;
        String configuredPath = System.getProperty(fileProperty);
        if (configuredPath != null && !configuredPath.isBlank()) {
            Path configuredFile = Path.of(configuredPath);
            assumeTrue(Files.isRegularFile(configuredFile),
                    "Configured DICOM test file is unavailable for " + transferSyntax);
            search = new SampleSearch();
            if (inspectCandidate(configuredFile, search, transferSyntax)) {
                search.source = configuredFile;
            }
        } else {
            assumeTrue(Files.isDirectory(REAL_US_TEST_DIRECTORY),
                    "Real DICOM test directory is unavailable: " + REAL_US_TEST_DIRECTORY);
            search = findRealMultiFrame(transferSyntax);
        }
        if (search.source == null) {
            throw new AssertionError("No matching Ultrasound Multi-frame DICOM found for "
                    + transferSyntax + ". "
                    + "Scanned files=" + search.files
                    + ", readable DICOM=" + search.readableDicom
                    + ", target SOP class=" + search.targetSopClass
                    + ", target SOP class with NumberOfFrames > 1=" + search.targetMultiFrame
                    + ", matching transfer syntax=" + search.matchingTransferSyntax
                    + ". Expected SOP Class 1.2.840.10008.5.1.4.1.1.3.1, "
                    + "NumberOfFrames > 1, Transfer Syntax " + transferSyntax + ".");
        }
        return search.source;
    }

    private static SampleSearch findRealMultiFrame(String transferSyntax) throws IOException {
        SampleSearch search = new SampleSearch();
        try (Stream<Path> files = Files.walk(REAL_US_TEST_DIRECTORY)) {
            java.util.Iterator<Path> paths = files.filter(Files::isRegularFile)
                    .sorted().iterator();
            while (paths.hasNext()) {
                Path path = paths.next();
                if (inspectCandidate(path, search, transferSyntax)) {
                    search.source = path;
                    return search;
                }
            }
        }
        return search;
    }

    private static boolean inspectCandidate(Path path, SampleSearch search,
                                            String expectedTransferSyntax) {
        search.files++;
        try (DicomInputStream dis = new DicomInputStream(path.toFile())) {
            dis.readFileMetaInformation();
            Attributes dataset = dis.readDatasetUntilPixelData();
            search.readableDicom++;

            if (!MaskMultiFrameUSTransformer.ULTRASOUND_MULTI_FRAME_SOP_CLASS_UID
                    .equals(dataset.getString(Tag.SOPClassUID))) {
                return false;
            }
            search.targetSopClass++;
            if (dataset.getInt(Tag.NumberOfFrames, 1) <= 1) {
                return false;
            }
            search.targetMultiFrame++;
            if (!expectedTransferSyntax.equals(dis.getTransferSyntax())) {
                return false;
            }
            search.matchingTransferSyntax++;
            return true;
        } catch (IOException | RuntimeException ignored) {
            // Non-DICOM files and unreadable instances are not candidates.
            return false;
        }
    }

    private static final class SampleSearch {
        private Path source;
        private int files;
        private int readableDicom;
        private int targetSopClass;
        private int targetMultiFrame;
        private int matchingTransferSyntax;
    }

    private static Attributes[] readFully(byte[] dicom) throws IOException {
        try (DicomInputStream dis = new DicomInputStream(new ByteArrayInputStream(dicom))) {
            Attributes fileMeta = dis.readFileMetaInformation();
            return new Attributes[]{fileMeta, dis.readDataset()};
        }
    }

    private static Attributes[] readHeader(byte[] dicom) throws IOException {
        try (DicomInputStream dis = new DicomInputStream(new ByteArrayInputStream(dicom))) {
            Attributes fileMeta = dis.readFileMetaInformation();
            return new Attributes[]{fileMeta, dis.readDatasetUntilPixelData()};
        }
    }

    private static byte[] createDicom(String sopClassUid, int frames, int samples,
                                      String photometricInterpretation, byte[] pixels)
            throws IOException {
        if (pixels.length != ROWS * COLUMNS * samples * frames) {
            throw new IllegalArgumentException("PixelData-Länge passt nicht zur Framezahl");
        }

        Attributes dataset = new Attributes();
        dataset.setString(Tag.SOPClassUID, VR.UI, sopClassUid);
        dataset.setString(Tag.SOPInstanceUID, VR.UI, UIDUtils.createUID());
        dataset.setString(Tag.StudyInstanceUID, VR.UI, UIDUtils.createUID());
        dataset.setString(Tag.SeriesInstanceUID, VR.UI, UIDUtils.createUID());
        dataset.setInt(Tag.Rows, VR.US, ROWS);
        dataset.setInt(Tag.Columns, VR.US, COLUMNS);
        dataset.setInt(Tag.SamplesPerPixel, VR.US, samples);
        dataset.setString(Tag.PhotometricInterpretation, VR.CS, photometricInterpretation);
        if (samples == 3) {
            dataset.setInt(Tag.PlanarConfiguration, VR.US, 0);
        }
        dataset.setInt(Tag.BitsAllocated, VR.US, 8);
        dataset.setInt(Tag.BitsStored, VR.US, 8);
        dataset.setInt(Tag.HighBit, VR.US, 7);
        dataset.setInt(Tag.PixelRepresentation, VR.US, 0);
        dataset.setString(Tag.NumberOfFrames, VR.IS, Integer.toString(frames));
        dataset.setBytes(Tag.PixelData, VR.OB, Arrays.copyOf(pixels, pixels.length));

        Attributes fileMeta = dataset.createFileMetaInformation(UID.ExplicitVRLittleEndian);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DicomOutputStream dos = new DicomOutputStream(bytes, UID.ExplicitVRLittleEndian)) {
            dos.writeDataset(fileMeta, dataset);
        }
        return bytes.toByteArray();
    }
}
